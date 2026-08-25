---------------------------- MODULE WaveJoinProbe ----------------------------
(*****************************************************************************)
(* Cooperative wave-probe contract for `RadixHashJoin`.                      *)
(*                                                                           *)
(* One shared wave exists at any time.  While it is ACTIVE, participating    *)
(* probe workers scan: each reserves the next input block against the byte   *)
(* budget and admits it (hashes its rows) into the wave.  The reservation    *)
(* whose atomic addition crosses BUDGET seals the wave; once every in-flight *)
(* reservation has been admitted, the same workers drain the sealed wave by  *)
(* claiming jobs from an explicit work graph:                                *)
(*                                                                           *)
(*     pre (per-pass accounting / range allocation)                          *)
(*   -> scatter (per admitted block, stable: row occurrence o lands in       *)
(*      pass-arena cell Rank0(o))                                            *)
(*   -> refalloc + refine (per pass; only when PL > 1)                       *)
(*   -> probe (per leaf: the smallest probe task; the executing worker       *)
(*      emits that leaf's results into ITS OWN output sequence)              *)
(*                                                                           *)
(* No dedicated producer or consumer crews exist: any worker claims any      *)
(* claimable job.  There is no shared output queue: `out[w]` is worker-local *)
(* and only ever appended by w itself.  EOF, the final partial wave, PL = 1  *)
(* and multi-wave runs all use this same machine.                            *)
(*                                                                           *)
(* Mapping to the C++ implementation:                                        *)
(*   worker w        = one probe lane executing `joinBlock`/`IJoinResult::   *)
(*                     next` quanta, or one delayed-blocks stream pull.      *)
(*   Reserve/Admit   = atomic fetch-add on the wave byte counter, then       *)
(*                     hashing + appending the block to the wave.            *)
(*   Claim/Finish    = atomic acquisition of a drain job and its execution.  *)
(*   out[w]          = the blocks returned by that lane's own result/stream. *)
(*   A caller that destroys its result early maps to a worker that finishes  *)
(*   or releases its current task and never claims again; the executor       *)
(*   contract guarantees the remaining lanes / the delayed-blocks stream     *)
(*   keep pulling, so the fairness assumptions below stay satisfied.         *)
(*                                                                           *)
(* Memory honesty (finding F5): `mem` accounts EXACTLY the admitted wave     *)
(* bytes plus in-flight reservation bytes.  BUDGET is the wave admission /   *)
(* sealing threshold, NOT a bound on total process resident memory: drain    *)
(* arenas, hash columns, per-worker input, allocator overhead and output     *)
(* live outside this counter.  The invariants state the accounted bound      *)
(* only: an active wave stays below BUDGET until the single crossing         *)
(* admission, so  mem <= BUDGET + MaxBlockBytes  always.                     *)
(*                                                                           *)
(* `ClaimEligible` is the participation hook (finding F1): the              *)
(* implementation contract is ClaimEligible(w, kind, id) = TRUE for all      *)
(* arguments — every worker may claim every job.  The negative witness       *)
(* configuration overrides it with a broken eligibility (a dedicated         *)
(* scanner crew plus leaf affinity) to demonstrate that                      *)
(* `ParticipationLive` is falsifiable.  All theorems below are stated for    *)
(* the contractual (total) eligibility.                                      *)
(*****************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Input,               \* Seq(Seq(HashValue)): probe blocks; one global
                         \* stream abstracts the racing per-lane streams
                         \* (admission order is nondeterministic anyway, and
                         \* every property below is insensitive to it).
    HashValue, Result, Error,
    Bytes(_),            \* block id -> Nat \ {0}
    Pass0(_), LeafOf(_), \* hash -> pass / leaf-within-pass
    RowResult(_),        \* occurrence -> Seq(Result): that row's join output
    ErrorOf(_),          \* leaf -> Error (injected probe failure identity)
    P0, PL, BUDGET, WORKERS,
    FailLeaves,          \* leaves whose probe execution faults
    FaultySteps,         \* subset of {"scan","pre","scatter","refalloc","refine"}
    CancelAllowed,       \* BOOLEAN: external cooperative cancellation enabled
    NoRow, NoError, NoJob, NoBlock, FaultError

VARIABLE st

vars == <<st>>

(**************************** Static derived sets ****************************)

Passes == 0 .. (P0 - 1)
Leaves == 0 .. (P0 * PL - 1)
WorkerIds == 0 .. (WORKERS - 1)
BlockIds == [i \in 1 .. Len(Input) |-> i]

SeqSet(s) == {s[i] : i \in DOMAIN s}
EmptySeq(n) == [i \in 1 .. n |-> NoRow]
Min(a, b) == IF a <= b THEN a ELSE b

BlockOccs(b) == [r \in 1 .. Len(Input[b]) |-> <<b, r>>]

RECURSIVE RowsOfBlocks(_)
RowsOfBlocks(bs) ==
    IF Len(bs) = 0
    THEN <<>>
    ELSE BlockOccs(Head(bs)) \o RowsOfBlocks(Tail(bs))

Occ == SeqSet(RowsOfBlocks(BlockIds))
HashOf(o) == Input[o[1]][o[2]]
Pid(o) == Pass0(HashOf(o))
Lid(o) == Pid(o) * PL + LeafOf(HashOf(o))

RECURSIVE SumBytes(_)
SumBytes(bs) ==
    IF Len(bs) = 0
    THEN 0
    ELSE Bytes(Head(bs)) + SumBytes(Tail(bs))

MaxBlockBytes ==
    IF Len(Input) = 0
    THEN 0
    ELSE LET sizes == {Bytes(b) : b \in 1 .. Len(Input)}
         IN CHOOSE m \in sizes : \A x \in sizes : x <= m

RECURSIVE PassPart(_, _)
PassPart(rs, p) ==
    IF Len(rs) = 0
    THEN <<>>
    ELSE IF Pid(Head(rs)) = p
         THEN <<Head(rs)>> \o PassPart(Tail(rs), p)
         ELSE PassPart(Tail(rs), p)

RECURSIVE LeafPart(_, _)
LeafPart(rs, l) ==
    IF Len(rs) = 0
    THEN <<>>
    ELSE IF Lid(Head(rs)) = l
         THEN <<Head(rs)>> \o LeafPart(Tail(rs), l)
         ELSE LeafPart(Tail(rs), l)

ExpectedPass(bs, p) == PassPart(RowsOfBlocks(bs), p)
ExpectedLeaf(bs, l) == LeafPart(RowsOfBlocks(bs), l)

Pos(rs, o) == CHOOSE i \in DOMAIN rs : rs[i] = o

Rank0(rs, o) ==
    Cardinality({i \in 1 .. Pos(rs, o) : Pid(rs[i]) = Pid(o)})

Rank1(rs, o) ==
    Cardinality({i \in 1 .. Pos(rs, o) : Lid(rs[i]) = Lid(o)})

LeavesOfPass(p) == {p * PL + q : q \in 0 .. (PL - 1)}

RECURSIVE DropNoRow(_)
DropNoRow(s) ==
    IF Len(s) = 0
    THEN <<>>
    ELSE IF Head(s) = NoRow
         THEN DropNoRow(Tail(s))
         ELSE <<Head(s)>> \o DropNoRow(Tail(s))

RECURSIVE ConcatResults(_)
ConcatResults(rs) ==
    IF Len(rs) = 0
    THEN <<>>
    ELSE RowResult(Head(rs)) \o ConcatResults(Tail(rs))

RECURSIVE CatSeqs(_)
CatSeqs(ws) ==
    IF Len(ws) = 0
    THEN <<>>
    ELSE Head(ws) \o CatSeqs(Tail(ws))

SeqCount(s, r) == Cardinality({i \in DOMAIN s : s[i] = r})

(********************************* Jobs *************************************)

PreJob(p) == [kind |-> "pre", id |-> p]
ScatterJob(b) == [kind |-> "scatter", id |-> b]
RefAllocJob(p) == [kind |-> "refalloc", id |-> p]
RefineJob(p) == [kind |-> "refine", id |-> p]
ProbeJob(l) == [kind |-> "probe", id |-> l]

(* The participation hook.  Contract: identically TRUE (any worker may claim
   any job).  See the header comment; only the negative witness overrides it. *)
ClaimEligible(w, kind, id) == TRUE

DrainPhaseSet == {"pre", "scatter", "refalloc", "refine", "probe"}

WaveRows == RowsOfBlocks(st.queue)

(* The work side of participation: jobs that exist and are not done, derived
   ONLY from phase + done-sets + queue — never from worker state. *)
ExistingJobs ==
    CASE st.phase = "pre" -> {PreJob(p) : p \in Passes \ st.preDone}
    []   st.phase = "scatter" ->
             {ScatterJob(b) : b \in SeqSet(st.queue) \ st.scatterDone}
    []   st.phase = "refalloc" -> {RefAllocJob(p) : p \in Passes \ st.refallocDone}
    []   st.phase = "refine" -> {RefineJob(p) : p \in Passes \ st.refineDone}
    []   st.phase = "probe" -> {ProbeJob(l) : l \in Leaves \ st.probeDone}
    []   OTHER -> {}

OwnedJobs == {st.wk[w].job : w \in WorkerIds} \ {NoJob}

UnownedClaimable ==
    IF st.cancelled THEN {} ELSE ExistingJobs \ OwnedJobs

Idle(w) ==
    /\ st.wk[w].job = NoJob
    /\ st.wk[w].res = NoBlock
    /\ ~st.wk[w].stopped

RECURSIVE InflightSum(_)
InflightSum(n) ==
    IF n = 0
    THEN 0
    ELSE (IF st.wk[n - 1].res # NoBlock THEN Bytes(st.wk[n - 1].res) ELSE 0)
         + InflightSum(n - 1)

InflightBytes == InflightSum(WORKERS)

RECURSIVE CatOutUpTo(_)
CatOutUpTo(n) ==
    IF n = 0
    THEN <<>>
    ELSE CatOutUpTo(n - 1) \o st.out[n - 1]

EmittedAll == CatOutUpTo(WORKERS)

(********************************** Init ************************************)

Init ==
    st = [phase |-> "active",
          nextBlock |-> 1,
          queue |-> <<>>,
          mem |-> 0,
          crossed |-> FALSE,
          cancelled |-> FALSE,
          primary |-> NoError,
          doneWaves |-> <<>>,
          hashCount |-> [b \in 1 .. Len(Input) |-> 0],
          arena0 |-> [p \in Passes |-> <<>>],
          arena1 |-> [l \in Leaves |-> <<>>],
          preDone |-> {},
          scatterDone |-> {},
          refallocDone |-> {},
          refineDone |-> {},
          probeDone |-> {},
          wk |-> [w \in WorkerIds |->
                     [res |-> NoBlock, job |-> NoJob, stopped |-> FALSE]],
          out |-> [w \in WorkerIds |-> <<>>],
          liveEntries |-> {},
          liveA0 |-> {},
          liveA1 |-> {},
          freedEntries |-> 0,
          freedA0 |-> 0,
          freedA1 |-> [l \in Leaves |-> 0]]

(***************************** Scan (active wave) ****************************)

(* Atomic budget reservation: the fetch-add.  Enabled only while the counter
   is below BUDGET; the addition that reaches or crosses BUDGET marks the
   wave crossed (sealed once all in-flight admissions land).  This is the
   only source of overshoot: mem < BUDGET before the add, so afterwards
   mem < BUDGET + Bytes(b) <= BUDGET + MaxBlockBytes. *)
Reserve(w) ==
    /\ st.phase = "active"
    /\ ~st.cancelled
    /\ Idle(w)
    /\ st.nextBlock <= Len(Input)
    /\ st.mem < BUDGET
    /\ LET b == st.nextBlock
           newmem == st.mem + Bytes(b)
       IN st' = [st EXCEPT
              !.nextBlock = @ + 1,
              !.mem = newmem,
              !.crossed = newmem >= BUDGET,
              !.wk[w].res = b]

(* Hash + admit the reserved block into the wave.  Allowed after the wave
   crossed: a reservation taken before the crossing still belongs to this
   wave and must land before the drain starts. *)
Admit(w) ==
    /\ st.phase = "active"
    /\ ~st.cancelled
    /\ st.wk[w].res # NoBlock
    /\ LET b == st.wk[w].res
       IN st' = [st EXCEPT
              !.queue = Append(@, b),
              !.hashCount[b] = @ + 1,
              !.liveEntries = @ \cup {b},
              !.wk[w].res = NoBlock]

(* The wave crossed the budget and every in-flight reservation has been
   admitted: begin the drain.  No admission or next-wave scan can overlap it
   from here on (Reserve and Admit are enabled only in "active"). *)
Seal ==
    /\ st.phase = "active"
    /\ ~st.cancelled
    /\ st.crossed
    /\ \A w \in WorkerIds : st.wk[w].res = NoBlock
    /\ st' = [st EXCEPT !.phase = "pre"]

(* EOF with a non-empty partial wave: drain it through the same machine. *)
EOFSeal ==
    /\ st.phase = "active"
    /\ ~st.cancelled
    /\ ~st.crossed
    /\ st.nextBlock > Len(Input)
    /\ \A w \in WorkerIds : st.wk[w].res = NoBlock
    /\ st.queue # <<>>
    /\ st' = [st EXCEPT !.phase = "pre"]

FinishInput ==
    /\ st.phase = "active"
    /\ ~st.cancelled
    /\ st.nextBlock > Len(Input)
    /\ st.queue = <<>>
    /\ \A w \in WorkerIds : st.wk[w].res = NoBlock
    /\ st' = [st EXCEPT !.phase = "done"]

(************************ Drain: claim and execute **************************)

Claim(w) ==
    /\ Idle(w)
    /\ \E j \in UnownedClaimable :
           /\ ClaimEligible(w, j.kind, j.id)
           /\ st' = [st EXCEPT !.wk[w].job = j]

(* Accounting + range allocation for one pass: the arena is sized exactly
   from the admission-time histograms. *)
FinishPre(w, p) ==
    /\ st.wk[w].job = PreJob(p)
    /\ st' = [st EXCEPT
           !.wk[w].job = NoJob,
           !.preDone = @ \cup {p},
           !.arena0[p] = EmptySeq(Len(ExpectedPass(st.queue, p))),
           !.liveA0 = @ \cup {p}]

PreBarrier ==
    /\ st.phase = "pre"
    /\ ~st.cancelled
    /\ st.preDone = Passes
    /\ st' = [st EXCEPT !.phase = "scatter"]

(* Stable scatter of one admitted block: each of its row occurrences goes to
   its rank slot in its pass arena.  Write ranges of distinct blocks are
   disjoint (RankInjective), so scatter jobs never race. *)
FinishScatter(w, b) ==
    /\ st.wk[w].job = ScatterJob(b)
    /\ LET occs == SeqSet(BlockOccs(b))
       IN /\ \A o \in occs : st.arena0[Pid(o)][Rank0(WaveRows, o)] = NoRow
          /\ st' = [st EXCEPT
                 !.wk[w].job = NoJob,
                 !.scatterDone = @ \cup {b},
                 !.arena0 =
                     [p \in Passes |->
                         [i \in DOMAIN st.arena0[p] |->
                             IF \E o \in occs :
                                    Pid(o) = p /\ Rank0(WaveRows, o) = i
                             THEN CHOOSE o \in occs :
                                      Pid(o) = p /\ Rank0(WaveRows, o) = i
                             ELSE st.arena0[p][i]]]]

(* After the scatter the buffered input blocks are released (exactly once).
   With PL = 1 the pass arenas ARE the leaf arenas: ownership transfers
   (not a free), and the drain goes straight to probing. *)
ScatterBarrier ==
    /\ st.phase = "scatter"
    /\ ~st.cancelled
    /\ st.scatterDone = SeqSet(st.queue)
    /\ IF PL = 1
       THEN st' = [st EXCEPT
                !.phase = "probe",
                !.liveEntries = {},
                !.freedEntries = @ + 1,
                !.arena1 = [l \in Leaves |-> st.arena0[l]],
                !.liveA1 = st.liveA0,
                !.liveA0 = {}]
       ELSE st' = [st EXCEPT
                !.phase = "refalloc",
                !.liveEntries = {},
                !.freedEntries = @ + 1]

FinishRefAlloc(w, p) ==
    /\ st.wk[w].job = RefAllocJob(p)
    /\ st' = [st EXCEPT
           !.wk[w].job = NoJob,
           !.refallocDone = @ \cup {p},
           !.arena1 =
               [l \in Leaves |->
                   IF l \in LeavesOfPass(p)
                   THEN EmptySeq(Len(ExpectedLeaf(st.queue, l)))
                   ELSE st.arena1[l]],
           !.liveA1 = @ \cup LeavesOfPass(p)]

RefAllocBarrier ==
    /\ st.phase = "refalloc"
    /\ ~st.cancelled
    /\ st.refallocDone = Passes
    /\ st' = [st EXCEPT !.phase = "refine"]

(* Refine one pass: read the ACTUAL pass-arena contents (not the expected
   value — a scatter defect must propagate) and place each row in its leaf
   rank slot. *)
FinishRefine(w, p) ==
    /\ st.wk[w].job = RefineJob(p)
    /\ LET occs == SeqSet(st.arena0[p]) \ {NoRow}
       IN /\ \A o \in occs : st.arena1[Lid(o)][Rank1(WaveRows, o)] = NoRow
          /\ st' = [st EXCEPT
                 !.wk[w].job = NoJob,
                 !.refineDone = @ \cup {p},
                 !.arena1 =
                     [l \in Leaves |->
                         [i \in DOMAIN st.arena1[l] |->
                             IF \E o \in occs :
                                    Lid(o) = l /\ Rank1(WaveRows, o) = i
                             THEN CHOOSE o \in occs :
                                      Lid(o) = l /\ Rank1(WaveRows, o) = i
                             ELSE st.arena1[l][i]]]]

RefineBarrier ==
    /\ st.phase = "refine"
    /\ ~st.cancelled
    /\ st.refineDone = Passes
    /\ st' = [st EXCEPT
           !.phase = "probe",
           !.liveA0 = {},
           !.freedA0 = @ + 1]

(* Probe one leaf: the smallest probe task.  The output is computed from the
   ACTUAL leaf-arena contents and appended to the executing worker's OWN
   output — there is no shared result buffer to model.  The leaf arena is
   released exactly once, here. *)
FinishProbe(w, l) ==
    /\ st.wk[w].job = ProbeJob(l)
    /\ l \notin FailLeaves
    /\ st' = [st EXCEPT
           !.wk[w].job = NoJob,
           !.probeDone = @ \cup {l},
           !.out[w] = @ \o ConcatResults(DropNoRow(st.arena1[l])),
           !.liveA1 = @ \ {l},
           !.freedA1[l] = @ + 1]

CompleteWave ==
    /\ st.phase = "probe"
    /\ ~st.cancelled
    /\ st.probeDone = Leaves
    /\ \A w \in WorkerIds : st.wk[w].job = NoJob
    /\ st' = [st EXCEPT
           !.phase = "active",
           !.doneWaves = Append(@, st.queue),
           !.queue = <<>>,
           !.mem = 0,
           !.crossed = FALSE,
           !.arena0 = [p \in Passes |-> <<>>],
           !.arena1 = [l \in Leaves |-> <<>>],
           !.preDone = {},
           !.scatterDone = {},
           !.refallocDone = {},
           !.refineDone = {},
           !.probeDone = {},
           !.freedEntries = 0,
           !.freedA0 = 0,
           !.freedA1 = [l \in Leaves |-> 0]]

(********************* Failure, cancellation, teardown ***********************)

(* First exception wins: `primary` is written only while it is NoError.
   Every fault makes cancellation visible and stops the faulting worker;
   its owned work is released exactly once by the same transition. *)

FaultScan(w) ==
    /\ "scan" \in FaultySteps
    /\ st.wk[w].res # NoBlock
    /\ st' = [st EXCEPT
           !.mem = @ - Bytes(st.wk[w].res),
           !.wk[w].res = NoBlock,
           !.wk[w].stopped = TRUE,
           !.cancelled = TRUE,
           !.primary = IF st.primary = NoError THEN FaultError ELSE @]

FaultStep(w) ==
    /\ st.wk[w].job # NoJob
    /\ st.wk[w].job.kind \in FaultySteps
    /\ st' = [st EXCEPT
           !.wk[w].job = NoJob,
           !.wk[w].stopped = TRUE,
           !.cancelled = TRUE,
           !.primary = IF st.primary = NoError THEN FaultError ELSE @]

FaultProbe(w, l) ==
    /\ st.wk[w].job = ProbeJob(l)
    /\ l \in FailLeaves
    /\ st' = [st EXCEPT
           !.wk[w].job = NoJob,
           !.wk[w].stopped = TRUE,
           !.cancelled = TRUE,
           !.primary = IF st.primary = NoError THEN ErrorOf(l) ELSE @,
           !.liveA1 = @ \ {l},
           !.freedA1[l] = @ + 1]

(* External cooperative cancellation: no error, just a visible request. *)
ExternalCancel ==
    /\ CancelAllowed
    /\ ~st.cancelled
    /\ st.phase \notin {"done", "failed"}
    /\ st' = [st EXCEPT !.cancelled = TRUE]

(* Once cancellation is visible no new work is claimed (Claim, Reserve,
   Admit and the barriers are all guarded on ~cancelled); owners unwind. *)
ReleaseJob(w) ==
    /\ st.cancelled
    /\ st.wk[w].job # NoJob
    /\ st' = [st EXCEPT !.wk[w].job = NoJob]

ReleaseRes(w) ==
    /\ st.cancelled
    /\ st.wk[w].res # NoBlock
    /\ st' = [st EXCEPT
           !.mem = @ - Bytes(st.wk[w].res),
           !.wk[w].res = NoBlock]

StopWorker(w) ==
    /\ st.cancelled
    /\ ~st.wk[w].stopped
    /\ st.wk[w].job = NoJob
    /\ st.wk[w].res = NoBlock
    /\ st' = [st EXCEPT !.wk[w].stopped = TRUE]

(* All participants reached a valid terminal state: release what is still
   live (each resource exactly once) and surface the primary error. *)
Teardown ==
    /\ st.cancelled
    /\ st.phase \notin {"done", "failed"}
    /\ \A w \in WorkerIds : st.wk[w].stopped
    /\ st' = [st EXCEPT
           !.phase = "failed",
           !.freedEntries = @ + (IF st.liveEntries # {} THEN 1 ELSE 0),
           !.freedA0 = @ + (IF st.liveA0 # {} THEN 1 ELSE 0),
           !.freedA1 = [l \in Leaves |->
                           IF l \in st.liveA1 THEN @[l] + 1 ELSE @[l]],
           !.liveEntries = {},
           !.liveA0 = {},
           !.liveA1 = {}]

(********************************** Next *************************************)

Terminating ==
    /\ st.phase \in {"done", "failed"}
    /\ UNCHANGED vars

WorkerStep(w) ==
    \/ Reserve(w)
    \/ Admit(w)
    \/ Claim(w)
    \/ \E p \in Passes : FinishPre(w, p)
    \/ \E b \in 1 .. Len(Input) : FinishScatter(w, b)
    \/ \E p \in Passes : FinishRefAlloc(w, p)
    \/ \E p \in Passes : FinishRefine(w, p)
    \/ \E l \in Leaves : FinishProbe(w, l)
    \/ FaultScan(w)
    \/ FaultStep(w)
    \/ \E l \in Leaves : FaultProbe(w, l)
    \/ ReleaseJob(w)
    \/ ReleaseRes(w)
    \/ StopWorker(w)

Transition ==
    \/ Seal
    \/ EOFSeal
    \/ FinishInput
    \/ PreBarrier
    \/ ScatterBarrier
    \/ RefAllocBarrier
    \/ RefineBarrier
    \/ CompleteWave
    \/ Teardown

Next ==
    \/ \E w \in WorkerIds : WorkerStep(w)
    \/ Transition
    \/ ExternalCancel
    \/ Terminating

Spec == Init /\ [][Next]_vars

(******************************* Assumptions *********************************)

EnvironmentOK ==
    /\ P0 \in Nat \ {0}
    /\ PL \in Nat \ {0}
    /\ BUDGET \in Nat \ {0}
    /\ WORKERS \in Nat \ {0}
    /\ HashValue # {}
    /\ Result # {}
    /\ Error # {}
    /\ Input \in Seq(Seq(HashValue))
    /\ \A b \in 1 .. Len(Input) : Bytes(b) \in Nat \ {0}
    /\ \A h \in HashValue : Pass0(h) \in Passes
    /\ \A h \in HashValue : LeafOf(h) \in 0 .. (PL - 1)
    /\ \A o \in Occ : RowResult(o) \in Seq(Result)
    /\ FailLeaves \subseteq Leaves
    /\ FaultySteps \subseteq {"scan", "pre", "scatter", "refalloc", "refine"}
    /\ CancelAllowed \in BOOLEAN
    /\ FaultError \in Error
    /\ \A l \in Leaves : ErrorOf(l) \in Error
    /\ NoError \notin Error
    /\ NoRow \notin Occ
    /\ NoBlock \notin 1 .. Len(Input)

ASSUME EnvironmentOK

(**************************** Safety invariants ******************************)

Phases == {"active", "pre", "scatter", "refalloc", "refine", "probe",
           "done", "failed"}

AllJobs ==
    {PreJob(p) : p \in Passes}
    \cup {ScatterJob(b) : b \in 1 .. Len(Input)}
    \cup {RefAllocJob(p) : p \in Passes}
    \cup {RefineJob(p) : p \in Passes}
    \cup {ProbeJob(l) : l \in Leaves}

TypeOK ==
    /\ st.phase \in Phases
    /\ st.nextBlock \in 1 .. (Len(Input) + 1)
    /\ st.queue \in Seq(1 .. Len(Input))
    /\ st.mem \in Nat
    /\ st.crossed \in BOOLEAN
    /\ st.cancelled \in BOOLEAN
    /\ st.primary \in Error \cup {NoError}
    /\ st.doneWaves \in Seq(Seq(1 .. Len(Input)))
    /\ st.hashCount \in [1 .. Len(Input) -> Nat]
    /\ st.arena0 \in [Passes -> Seq(Occ \cup {NoRow})]
    /\ st.arena1 \in [Leaves -> Seq(Occ \cup {NoRow})]
    /\ st.preDone \subseteq Passes
    /\ st.scatterDone \subseteq 1 .. Len(Input)
    /\ st.refallocDone \subseteq Passes
    /\ st.refineDone \subseteq Passes
    /\ st.probeDone \subseteq Leaves
    /\ st.wk \in [WorkerIds ->
                     [res : (1 .. Len(Input)) \cup {NoBlock},
                      job : AllJobs \cup {NoJob},
                      stopped : BOOLEAN]]
    /\ st.out \in [WorkerIds -> Seq(Result)]
    /\ st.liveEntries \subseteq 1 .. Len(Input)
    /\ st.liveA0 \subseteq Passes
    /\ st.liveA1 \subseteq Leaves
    /\ st.freedEntries \in Nat
    /\ st.freedA0 \in Nat
    /\ st.freedA1 \in [Leaves -> Nat]

(* F5: the accounted bytes are exactly the admitted wave plus in-flight
   reservations, the threshold overshoot is bounded by one block, and the
   crossed flag is equivalent to the counter having reached BUDGET. *)
MemAccounted ==
    st.phase # "failed" => st.mem = SumBytes(st.queue) + InflightBytes

MemBound == st.mem <= BUDGET + MaxBlockBytes

CrossedSound ==
    (~st.cancelled /\ st.phase \notin {"done", "failed"})
        => (st.crossed <=> st.mem >= BUDGET)

(* Exactly one shared wave: no admission or reservation overlaps a drain. *)
NoAdmitDuringDrain ==
    st.phase \in DrainPhaseSet => \A w \in WorkerIds : st.wk[w].res = NoBlock

AdmittedSet == SeqSet(CatSeqs(st.doneWaves)) \cup SeqSet(st.queue)

HashOnce ==
    \A b \in 1 .. Len(Input) :
        st.hashCount[b] = IF b \in AdmittedSet THEN 1 ELSE 0

(* Every completed wave was sealed for a reason: it crossed the budget, or it
   is the final EOF wave. *)
WaveJustified ==
    \A i \in 1 .. Len(st.doneWaves) :
        \/ SumBytes(st.doneWaves[i]) >= BUDGET
        \/ (i = Len(st.doneWaves) /\ st.nextBlock = Len(Input) + 1)

SealedJustified ==
    st.phase \in DrainPhaseSet =>
        \/ SumBytes(st.queue) >= BUDGET
        \/ st.nextBlock = Len(Input) + 1

(* F1: the ownership side (worker-local state) cross-checked against the
   independently maintained work side (phase + done-sets + queue). *)
OwnershipConsistent ==
    /\ \A w1, w2 \in WorkerIds :
           (w1 # w2 /\ st.wk[w1].job # NoJob) => st.wk[w1].job # st.wk[w2].job
    /\ \A w \in WorkerIds :
           st.wk[w].job # NoJob => st.wk[w].job \in ExistingJobs
    /\ \A w \in WorkerIds : st.wk[w].res # NoBlock => st.phase = "active"
    /\ \A w \in WorkerIds :
           st.wk[w].stopped => (st.wk[w].job = NoJob /\ st.wk[w].res = NoBlock)

Footprint(j) ==
    CASE j.kind = "pre" -> {<<"a0", j.id>>}
    []   j.kind = "scatter" ->
             {<<"a0cell", Pid(o), Rank0(WaveRows, o)>> :
                  o \in SeqSet(BlockOccs(j.id))}
    []   j.kind = "refalloc" -> {<<"a1", l>> : l \in LeavesOfPass(j.id)}
    []   j.kind = "refine" ->
             {<<"a0", j.id>>} \cup {<<"a1", l>> : l \in LeavesOfPass(j.id)}
    []   j.kind = "probe" -> {<<"a1", j.id>>, <<"ht", j.id>>}
    []   OTHER -> {}

RaceFree ==
    \A w1, w2 \in WorkerIds :
        (/\ w1 # w2
         /\ st.wk[w1].job # NoJob
         /\ st.wk[w2].job # NoJob)
            => Footprint(st.wk[w1].job) \cap Footprint(st.wk[w2].job) = {}

RankInjective ==
    /\ \A o1, o2 \in SeqSet(WaveRows) :
           /\ Pid(o1) = Pid(o2)
           /\ Rank0(WaveRows, o1) = Rank0(WaveRows, o2)
           => o1 = o2
    /\ \A o1, o2 \in SeqSet(WaveRows) :
           /\ Lid(o1) = Lid(o2)
           /\ Rank1(WaveRows, o1) = Rank1(WaveRows, o2)
           => o1 = o2

CellSafety ==
    /\ \A p \in Passes :
           \A i \in DOMAIN st.arena0[p] :
               \/ st.arena0[p][i] = NoRow
               \/ st.arena0[p][i] = ExpectedPass(st.queue, p)[i]
    /\ \A l \in Leaves :
           \A i \in DOMAIN st.arena1[l] :
               \/ st.arena1[l][i] = NoRow
               \/ st.arena1[l][i] = ExpectedLeaf(st.queue, l)[i]

CapacityExact ==
    /\ \A p \in st.liveA0 :
           Len(st.arena0[p]) = Len(ExpectedPass(st.queue, p))
    /\ \A l \in st.liveA1 :
           Len(st.arena1[l]) = Len(ExpectedLeaf(st.queue, l))

StableAtBarriers ==
    /\ st.phase \in {"refalloc", "refine"} =>
           \A p \in Passes : st.arena0[p] = ExpectedPass(st.queue, p)
    /\ st.phase = "probe" =>
           \A l \in Leaves : st.arena1[l] = ExpectedLeaf(st.queue, l)

(* Running refinement: everything emitted so far is justified by rows already
   admitted (each row's results at most once). *)
ExpectedSoFar == ConcatResults(RowsOfBlocks(CatSeqs(st.doneWaves) \o st.queue))

OutputJustified ==
    \A r \in Result : SeqCount(EmittedAll, r) <= SeqCount(ExpectedSoFar, r)

(* F6: end-to-end multiset refinement, checked as an invariant by TLC.  At
   "done" the union of the per-worker outputs equals, as a bag, the join
   output of every input row — probed exactly once, no drops, no
   duplicates, no cross-wiring; order is deliberately unconstrained. *)
AllInputResults == ConcatResults(RowsOfBlocks(BlockIds))

FinalRefinement ==
    st.phase = "done" =>
        \A r \in Result :
            SeqCount(EmittedAll, r) = SeqCount(AllInputResults, r)

FreedOnce ==
    /\ st.freedEntries <= 1
    /\ st.freedA0 <= 1
    /\ \A l \in Leaves : st.freedA1[l] <= 1

TerminalClean ==
    /\ st.phase = "done" =>
           /\ ~st.cancelled
           /\ st.queue = <<>>
           /\ st.liveEntries = {} /\ st.liveA0 = {} /\ st.liveA1 = {}
           /\ \A w \in WorkerIds :
                  /\ st.wk[w].job = NoJob
                  /\ st.wk[w].res = NoBlock
                  /\ ~st.wk[w].stopped
    /\ st.phase = "failed" =>
           /\ st.cancelled
           /\ st.liveEntries = {} /\ st.liveA0 = {} /\ st.liveA1 = {}
           /\ \A w \in WorkerIds : st.wk[w].stopped

FailureSafety ==
    /\ \A w \in WorkerIds : st.wk[w].stopped => st.cancelled
    /\ st.primary # NoError => st.cancelled

Safety ==
    /\ TypeOK
    /\ MemAccounted
    /\ MemBound
    /\ CrossedSound
    /\ NoAdmitDuringDrain
    /\ HashOnce
    /\ WaveJustified
    /\ SealedJustified
    /\ OwnershipConsistent
    /\ RaceFree
    /\ RankInjective
    /\ CellSafety
    /\ CapacityExact
    /\ StableAtBarriers
    /\ OutputJustified
    /\ FinalRefinement
    /\ FreedOnce
    /\ TerminalClean
    /\ FailureSafety

(************************ Fairness and liveness *******************************)

(* Stated fairness: every participating worker keeps being scheduled (each
   lane's joinBlock/next quanta and the delayed-blocks pulls keep arriving),
   and the bounded phase transitions execute.  External cancellation is NOT
   assumed to happen. *)
Fairness ==
    /\ \A w \in WorkerIds : WF_vars(WorkerStep(w))
    /\ WF_vars(Transition)

FairSpec == Spec /\ Fairness

Termination == <>(st.phase \in {"done", "failed"})

(* Non-vacuous work conservation (F1): an idle worker facing existing
   unowned work (judged against the CONTRACT: any phase-compatible unowned
   job, or open scanning — independent of any eligibility restriction)
   eventually acquires work, unless the work is consumed by others or the
   run is cancelled.  Falsified by the dedicated-crew witness. *)
ScanOpen ==
    /\ st.phase = "active"
    /\ ~st.cancelled
    /\ st.mem < BUDGET
    /\ st.nextBlock <= Len(Input)

WorkAvailable(w) ==
    /\ Idle(w)
    /\ ~st.cancelled
    /\ (UnownedClaimable # {} \/ ScanOpen)

Acquired(w) == st.wk[w].job # NoJob \/ st.wk[w].res # NoBlock

ParticipationLive ==
    \A w \in WorkerIds :
        [](WorkAvailable(w) =>
               <>(\/ Acquired(w)
                  \/ (UnownedClaimable = {} /\ ~ScanOpen)
                  \/ st.cancelled))

(* First exception wins, as an action property: once primary is set it is
   never overwritten. *)
PrimaryStable == [][st.primary # NoError => st'.primary = st.primary]_vars

THEOREM SafetyTheorem == Spec => []Safety
THEOREM PrimaryTheorem == Spec => PrimaryStable
THEOREM TerminationTheorem == FairSpec => Termination
THEOREM ParticipationTheorem == FairSpec => ParticipationLive

(* If implementation evidence falsifies EnvironmentOK or the fairness
   mapping: halt, name the falsified conjunct, propose the minimum revision,
   and do not add a fallback path. *)

=============================================================================
