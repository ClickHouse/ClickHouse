(ns jepsen.clickhouse.os
  "Ubuntu OS setup that makes apt wait, rather than fail, for the things that
  keep an installation from starting or finishing, and gives up on the ones it
  cannot wait for.

  apt's default lock timeout is 0, so an apt-get that finds a lock held exits
  immediately rather than waiting. A node is booted seconds before a test runs
  it, while the automatic apt jobs of a fresh boot still hold those locks, so
  jepsen.os.ubuntu/setup! needs apt configured to wait before it installs.

  The archive apt downloads from also fails on its own: an EC2 Ubuntu mirror
  rate-limits, answers 503, or drops the connection, and apt exits 100 having
  fetched nothing. Every test in a run sets its nodes up again, so a mirror
  unavailable for minutes crashes the tests that start inside that window and
  none of the rest, which is why the installation is retried here rather than
  left to fail the test.

  A mirror that accepts the connection and then answers nothing is bounded
  rather than waited for. apt's own timeout is 120 s, spent per target of an
  update and per package of an install and multiplied by apt's own retries, so
  an update of 22 indexes against such a mirror takes hours to report anything
  at all - longer than the job is allowed to run. The timeouts apt is
  configured with here, and the deadline the update is run under, are what turn
  that into a failure this can retry.

  An apt the node started for itself against such a mirror is bounded the only
  way it can be: it holds the lists lock, none of the configuration above
  reached it, and it is stopped. Waiting for it costs the whole job rather than
  one test, because every test in the run waits for the same lock again."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.os :as os]
            [jepsen.os.debian :as debian]
            [jepsen.os.ubuntu :as ubuntu]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]
            [jepsen.util :as util]
            [clj-commons.slingshot :refer [try+ throw+]]))

(def apt-conf-path
  "Where the options apt is configured with are written. One drop-in for all of
  them, so a node either has the whole set or none of it."
  "/etc/apt/apt.conf.d/99-clickhouse-jepsen")

(def apt-lock-timeout-seconds
  "How long apt waits for a lock it needs, and how long the lists lock, which
  apt has no option to wait for, is waited for here."
  600)

(def apt-acquire-timeout-seconds
  "How long apt waits on a mirror that accepted the connection: for it to start
  answering, and for the next bytes of an answer it has begun. apt's default is
  120 s, and it is spent once per target of an update, so a mirror that answers
  nothing holds an update of 22 indexes for the better part of an hour, and for
  as many hours as apt retries."
  30)

(def apt-acquire-retries
  "How many times apt retries a download of its own. The setup is retried as a
  whole here, so apt's own retries only multiply how long an attempt takes to
  report that the mirror is unavailable."
  1)

(def apt-options
  "The options apt is configured with, spelled as apt writes them in a
  configuration file and reports them back from `apt-config dump`."
  [["DPkg::Lock::Timeout" apt-lock-timeout-seconds]
   ["Acquire::http::Timeout" apt-acquire-timeout-seconds]
   ["Acquire::https::Timeout" apt-acquire-timeout-seconds]
   ["Acquire::Retries" apt-acquire-retries]])

(defn apt-option-line
  "The line apt writes an option as, in a configuration file and in a dump."
  [[option value]]
  (str option " \"" value "\";"))

(def apt-conf-content
  (str (str/join "\n" (map apt-option-line apt-options)) "\n"))

(def apt-update-timeout-seconds
  "How long one apt-get update is given before it is killed, so that another
  attempt can be made instead. A mirror that needs longer than this for the
  package indexes is answering slowly enough that asking it again is worth more
  than waiting for it, and a mirror that answers nothing at all would otherwise
  hold the update for apt-acquire-timeout-seconds per target and per retry -
  long enough to spend the whole job on a single attempt. Only an update is
  killed and only its downloads are lost: no dpkg runs during one, so there is
  nothing half-done for the next apt to find."
  300)

(def apt-kill-after-seconds
  "How long after the deadline an apt-get that has not exited is killed."
  10)

(def apt-archive-retry-seconds
  "How long the run waits, in total, for an archive it cannot download from.
  The same budget apt is given for the locks it waits for."
  apt-lock-timeout-seconds)

(def apt-lists-lock-grace-seconds
  "How long the process found holding the lists lock is left to finish before it
  is stopped. A node boots seconds before a test runs it, so the `apt-get
  update` of its own boot is often the incumbent, and against an archive that
  answers it finishes in seconds - the CI logs of a healthy node show four of
  them. One still holding the lock after this long is downloading from an
  archive that has stopped answering, and none of the options above reached it:
  it was started before the drop-in was written, with apt's own 120 s per target
  and apt's own retries on top, so it holds the lock for hours. Waiting for it
  is not one test's cost but the whole run's, because every test sets its nodes
  up again and waits for the same lock."
  60)

(def apt-lock-retry-interval-ms 5000)

(def apt-archive-retry-interval-ms 15000)

(def apt-lists-lock-path
  "The lock apt takes while it fetches the package lists."
  "/var/lib/apt/lists/lock")

(def lists-lock-error
  "What apt prints when /var/lib/apt/lists/lock is held. DPkg::Lock::Timeout
  does not cover this lock, so it needs its own retry."
  (str "Could not get lock " apt-lists-lock-path))

(def lists-lock-holder-pattern
  "How apt names what holds the lists lock. apt reads the holder's id out of the
  lock and its name out of `/proc`, so both are there to be read back, and a
  holder that is not an apt fetching package lists can be told apart."
  (re-pattern (str (java.util.regex.Pattern/quote lists-lock-error)
                   "\\. It is held by process (\\d+) \\(([^)]*)\\)")))

(def lists-lock-holders-to-stop
  "The names apt reports for a holder of the lists lock that this may stop. Only
  apt's own front-ends: the lists lock is taken to fetch package lists and for
  nothing else, and no dpkg runs while it is held, so a holder of it that is
  stopped leaves nothing half-done - only downloads to make again, which is what
  the update waiting for it is about to do anyway. A holder of any other name is
  left alone and waited for, as before."
  #{"apt-get" "apt" "aptitude"})

(def lists-lock-holder-signals
  "The signals sent, in this order, to a holder of the lists lock whose grace has
  run out. TERM first, so apt exits by its own hand and takes its partial
  downloads with it; KILL if the next attempt finds it still there, which gives
  TERM the interval between two attempts to be answered."
  [:TERM :KILL])

(def archive-fetch-errors
  "What apt prints when it could not download a file the package lists name.
  Nothing about the node or the request is wrong in that case, so these are
  worth another attempt: the mirror is unavailable, overloaded, or unresolvable
  for now. `Unable to fetch some archives` is the summary apt adds to every one
  of them, so on its own it identifies the class; the per-item lines are listed
  too, so a message carrying only those still matches."
  ["Unable to fetch some archives"
   "Failed to fetch"
   "Could not resolve"
   "Temporary failure resolving"])

(def stale-index-errors
  "What apt prints when the file it asked for is not the file the archive has.
  The download failed, but an unavailable mirror is not why: the package lists
  name a version the archive has already superseded, and apt says as much in
  `maybe run apt-get update`. Retrying the install cannot fix it, because
  jepsen.os.debian/maybe-update! inside the Ubuntu setup finds the lists young
  enough to keep - they are ours, fetched minutes ago, and stale all the same.
  Matched with patterns because apt's spacing between a status and its reason
  varies."
  [#"404\s+Not Found"
   #"Hash Sum mismatch"
   #"File has unexpected size"])

(def apt-boot-units
  "The units that run apt on a node of their own accord: the timers that would
  start one, and the service one of them has often already started, a node
  being booted seconds before a test runs it.

  `apt-daily.service` only downloads - the package lists, an unattended upgrade
  it asks for with `--download-only`, and an autoclean - so stopping it cannot
  interrupt a dpkg. `apt-daily-upgrade.service` and `unattended-upgrades.service`
  install, so they are left running and waited for instead: a dpkg stopped
  halfway leaves the node refusing every package operation until someone runs
  `dpkg --configure -a`, and DPkg::Lock::Timeout already waits for the lock they
  hold."
  [:apt-daily.timer :apt-daily-upgrade.timer :apt-daily.service])

(defn stop-apt-boot-jobs!
  "Stops the apt runs a boot starts. Best effort: a node without systemd, or
  with these units absent, is fine."
  []
  (try+
   (c/su (apply c/exec :systemctl :stop apt-boot-units))
   (catch Object _
     (warn "Could not stop the apt jobs of the boot; continuing"))))

(defn assert-apt-options!
  "Asserts apt parses the drop-in and resolves every option to the value
  written. Matching a whole dumped line is what distinguishes an effective
  drop-in from an inert one: apt reports its own compiled-in defaults for these
  options too, so a match on an option name alone succeeds even with no drop-in
  at all. apt-config echoes back an unknown key as well, so this establishes
  that apt reads the file, not that apt honours what it read; that the lock
  timeout takes effect under contention is what the install measurement shows.
  Catches a malformed drop-in (apt-config then exits 100) and an inert one (no
  matching line). Takes no lock, so it cannot block."
  []
  (let [dumped (try+
                ;; As root: apt skips a config file it cannot read, with a warning and exit 0.
                (c/su (c/exec :apt-config :dump))
                (catch [:type :jepsen.control/nonzero-exit] {:keys [err]}
                  (throw+ {:type ::apt-conf-unusable
                           :err err}
                          nil
                          "apt cannot read its configuration, so %s does not parse:\n%s"
                          apt-conf-path err)))
        reported (set (str/split-lines dumped))
        missing (remove reported (map apt-option-line apt-options))]
    (when (seq missing)
      (throw+ {:type ::apt-options-not-in-effect
               :missing missing}
              nil
              "apt does not report %s, so %s is not in effect"
              (str/join " " missing) apt-conf-path))
    (info "apt reports" (str/join " " (map apt-option-line apt-options)))))

(def apt-updated-hosts
  "Hosts on which this process has completed a setup, so their package lists
  were fetched by an update we issued and saw succeed. Held in memory because
  the node carries no state we can trust to be from this run, and read before
  any apt call: apt's own caches are refreshed even by an update that failed to
  take a lock and fetched nothing."
  (atom #{}))

(def archive-retry-deadline
  "When the retrying of an archive that cannot be downloaded from stops, as a
  wall clock time in milliseconds, or nil while nothing has had to retry.

  The budget is one window over the run rather than one per setup. Every test
  sets its nodes up again, so an archive that is down rather than busy would
  cost a full budget per test, and a run of 25 tests would then exceed the time
  the job is given instead of reporting what it found. The window is opened by
  the first attempt that has to retry, is spent by every node and every test at
  once, and is re-armed by a setup that completes after it has closed - so an
  outage later in the run is waited out again, while one that never ends is
  waited out once."
  (atom nil))

(defn may-retry-archive?
  "Opens the retry window if nothing has had to retry yet, and answers whether
  it still has time left in it."
  []
  (< (System/currentTimeMillis)
     (swap! archive-retry-deadline
            #(or % (+ (System/currentTimeMillis)
                      (* 1000 apt-archive-retry-seconds))))))

(defn rearm-archive-retries!
  "Re-arms the retry window if it has run out, so an outage later in the run is
  waited out again. Leaves a window that still has time in it alone: the nodes
  are set up in parallel, and one node's setup completing is not another node's
  archive answering."
  []
  (swap! archive-retry-deadline
         (fn [deadline]
           (when (and deadline (< (System/currentTimeMillis) deadline))
             deadline))))

(defn apt-error
  "The message apt printed for a failed apt-get, or nil when it was stopped
  before it could print one."
  [{:keys [err]}]
  (when (and (string? err) (not (str/blank? err)))
    err))

(defn killed-by-deadline?
  "Was this apt-get update killed by the deadline it was run under rather than
  finished? `timeout` reports 124 for the signal it sends, and 137 for the KILL
  it follows up with when the command does not take the first one. Asked of an
  update alone: nothing else here is run under a deadline, so the same status
  from an install is a signal from elsewhere - the OOM killer, say - and is
  neither ours nor an archive's fault to retry."
  [{:keys [exit]}]
  (contains? #{124 137} exit))

(defn failure-message
  "What to report for a failed apt-get: apt's own message, or its exit status
  when it was stopped before it could print one."
  [{:keys [exit] :as failure}]
  (or (apt-error failure)
      (str "apt-get exited with status " exit " without printing a message")))

(defn update-failure-message
  "What to report for a failed apt-get update, which is the only apt-get run
  under a deadline, and so the only one whose missing message has an answer."
  [{:keys [exit] :as failure}]
  (if (killed-by-deadline? failure)
    (str "apt-get update did not finish in " apt-update-timeout-seconds
         " s and was killed, exit status " exit)
    (failure-message failure)))

(defn lists-lock-held?
  "Did this apt-get fail because another apt holds /var/lib/apt/lists/lock?"
  [failure]
  (boolean (when-let [err (apt-error failure)]
             (str/includes? err lists-lock-error))))

(defn lists-lock-holder
  "What apt named as holding the lists lock, as {:pid :name}, or nil when the
  message named none. apt truncates the name to what `/proc` holds, so it is
  compared and reported and never used to find the process again: the id apt
  read out of the lock is what identifies it."
  [failure]
  (when-let [err (apt-error failure)]
    (when-let [[_ pid name] (re-find lists-lock-holder-pattern err)]
      {:pid (parse-long pid) :name name})))

(defn lists-lock-holder-signal
  "The next signal to send to a holder that has already been sent this many, or
  nil once the list is spent - a process that answers neither TERM nor KILL is
  waited for like any other holder, and reported at the deadline."
  [signals-sent]
  (get lists-lock-holder-signals signals-sent))

(defn describe-lists-lock-holder
  "How a holder is named in the log, or how one apt did not name is."
  [{:keys [pid], holder-name :name}]
  (if pid
    (str "process " pid " (" holder-name ")")
    "a process apt did not name"))

(defn stop-lists-lock-holder!
  "Sends a signal to the process apt named as holding the lists lock. `kill`
  exits non-zero for a process that has since gone, which is the outcome wanted
  rather than a failure, so nothing is thrown for it."
  [signal {:keys [pid] :as holder}]
  (info "sending" (name signal) "to" (describe-lists-lock-holder holder)
        "holding" apt-lists-lock-path "on" c/*host*)
  (try+
   (c/su (c/exec :kill (str "-" (name signal)) (str pid)))
   (catch [:type :jepsen.control/nonzero-exit] _
     (info "process" pid "was already gone"))))

(defn archive-unavailable?
  "Is this failed apt-get a failure to download from the archive, rather than a
  rejection of what was asked for? Matched on the message, because apt reports
  both with exit status 100."
  [failure]
  (boolean (when-let [err (apt-error failure)]
             (some #(str/includes? err %) archive-fetch-errors))))

(defn stale-index?
  "Did this download fail because the package lists name a file the archive
  does not have, rather than because the archive could not be reached?"
  [failure]
  (boolean (when-let [err (apt-error failure)]
             (some #(re-find % err) stale-index-errors))))

(defn readable-package-index-count
  "How many package indexes apt can currently read. `apt-get indextargets` lists
  any or none with the same exit status, so the count is the answer and the status
  is not. Counts only the records apt created from a Packages target: the
  auxiliary targets (Translations, DEP-11, CNF) are listed alongside them and come
  from the same Release file, so a node with no package index at all still lists
  those and cannot install anything. Reports what is on the node rather than
  whether every source was reachable, and takes no lock, so it neither blocks nor
  rejects a node that fetched some suites and not others. Sources apt refuses to
  parse are no readable index either, so that counts as none rather than failing
  the count."
  []
  (try+
   (count (filter #(= % "Created-By: Packages")
                  (str/split-lines (c/exec :apt-get :indextargets))))
   (catch [:type :jepsen.control/nonzero-exit] _
     0)))

(defn update-package-lists!
  "Runs one apt-get update on this host, under a deadline, retrying while the
  reason it failed is one that can pass: the lists lock held by another apt, or
  an archive that could not be downloaded from while the node still has no
  package index to install from. The update's exit status is the oracle in
  neither direction: apt exits 0 having fetched nothing when every source is
  unreachable, and exits non-zero having fetched every index that matters when
  one configured source is broken. The readable package indexes it leaves
  behind are what decides, so a failure the retrying gives up on is judged by
  that count rather than being fatal on its own.

  A holder of the lists lock that outlasts its grace is stopped rather than
  waited for, so that the archive it is stuck on costs this run one grace and
  not one lock timeout per test."
  []
  (let [start (System/currentTimeMillis)
        lock-deadline (+ start (* 1000 apt-lock-timeout-seconds))
        grace-deadline (+ start (* 1000 apt-lists-lock-grace-seconds))
        swallowed-err
        (loop [attempt 1
               signals-sent {}]
          (let [failure
                (try+
                 ;; debian/update! hardcodes its argv, so its lock is taken here.
                 (util/with-named-lock debian/node-locks c/*host*
                   (c/su (c/exec :timeout
                                 (str "--kill-after=" apt-kill-after-seconds)
                                 (str apt-update-timeout-seconds)
                                 :apt-get
                                 :--allow-releaseinfo-change :update)))
                 nil
                 (catch [:type :jepsen.control/nonzero-exit] failure
                   failure))]
            (cond
              (nil? failure)
              nil

              (lists-lock-held? failure)
              (if (< (System/currentTimeMillis) lock-deadline)
                (let [holder (lists-lock-holder failure)
                      signal (when (and holder
                                        (contains? lists-lock-holders-to-stop
                                                   (:name holder))
                                        (<= grace-deadline (System/currentTimeMillis)))
                               (lists-lock-holder-signal
                                (get signals-sent (:pid holder) 0)))]
                  (when (= 1 attempt)
                    (info "waiting for" apt-lists-lock-path "on" c/*host*
                          "held by" (describe-lists-lock-holder holder)))
                  (when signal
                    (stop-lists-lock-holder! signal holder))
                  (Thread/sleep (long apt-lock-retry-interval-ms))
                  (recur (inc attempt)
                         (if signal
                           (update signals-sent (:pid holder) (fnil inc 0))
                           signals-sent)))
                (let [waited (- (System/currentTimeMillis) start)]
                  (throw+ {:type ::apt-lists-lock-unavailable
                           :node c/*host*
                           :waited-ms waited
                           :err (apt-error failure)}
                          nil
                          "apt could not take %s on %s after %d ms:\n%s"
                          apt-lists-lock-path c/*host* waited
                          (update-failure-message failure))))

              ;; An archive that could not be downloaded from, with nothing on
              ;; the node to install from either: worth another attempt while
              ;; the run still has time for one.
              (and (or (killed-by-deadline? failure)
                       (archive-unavailable? failure))
                   (zero? (readable-package-index-count))
                   (may-retry-archive?))
              (do (warn "apt could not fetch the package lists on" c/*host*
                        (str "(attempt " attempt "), retrying:\n")
                        (update-failure-message failure))
                  (Thread/sleep (long apt-archive-retry-interval-ms))
                  (recur (inc attempt) signals-sent))

              :else
              (update-failure-message failure))))]
    (let [indexes (readable-package-index-count)]
      (when (zero? indexes)
        (throw+ {:type ::apt-package-lists-empty
                 :node c/*host*
                 :index-count indexes
                 :err swallowed-err}
                nil
                "apt reports no readable package index on %s:\n%s"
                c/*host*
                (or swallowed-err "the update apt called successful fetched none")))
      (when swallowed-err
        (warn "apt-get update failed on" c/*host* "but left" indexes
              "readable package indexes, so setup continues:\n" swallowed-err))
      (info "apt package lists fetched after"
            (- (System/currentTimeMillis) start) "ms," indexes "package indexes"))))

(defn ensure-package-lists!
  "Fetches the package lists of a host this process has not completed a setup
  on."
  []
  (when-not (contains? @apt-updated-hosts c/*host*)
    (update-package-lists!)))

(defn setup-packages!
  "Runs the Ubuntu setup, retrying while apt could not download what it needs.
  Retries the whole setup and not an install of our own listing of the packages,
  so the packages covered stay whatever the Ubuntu setup installs; each attempt
  asks apt only for what is still missing, so a retry resumes rather than
  restarts. Only a failure to download is retried - a package apt refuses to
  install is reported at once, as before. A download that failed because the
  package lists name a file the archive no longer has is not a mirror to wait
  for: the lists are fetched again, once, and a second such failure is reported
  rather than retried, since the update inside the Ubuntu setup keeps lists this
  young and no number of attempts would ask for anything else. Leaves apt's own
  message in the exception it throws when the run is out of retries, so a mirror
  that is broken rather than busy is diagnosed from it."
  [test node]
  (let [start (System/currentTimeMillis)]
    (loop [attempt 1
           refetched-lists? false]
      (when-let [failure (try+
                          (os/setup! ubuntu/os test node)
                          nil
                          (catch [:type :jepsen.control/nonzero-exit] failure
                            (if (archive-unavailable? failure)
                              failure
                              (throw+))))]
        (cond
          (stale-index? failure)
          (if refetched-lists?
            (throw+ {:type ::apt-package-lists-stale
                     :node c/*host*
                     :attempts attempt
                     :err (apt-error failure)}
                    nil
                    "apt could not download a package on %s that the package lists it just fetched name:\n%s"
                    c/*host* (failure-message failure))
            (do (warn "apt could not download a package the package lists on" c/*host*
                      "name, so they are fetched again:\n" (failure-message failure))
                (update-package-lists!)
                (recur (inc attempt) true)))

          (may-retry-archive?)
          (do (warn "apt could not download a package on" c/*host*
                    (str "(attempt " attempt "), retrying:\n")
                    (failure-message failure))
              (Thread/sleep (long apt-archive-retry-interval-ms))
              (recur (inc attempt) refetched-lists?))

          :else
          (let [waited (- (System/currentTimeMillis) start)]
            (throw+ {:type ::apt-archives-unreachable
                     :node c/*host*
                     :attempts attempt
                     :waited-ms waited
                     :err (apt-error failure)}
                    nil
                    "apt could not download a package on %s in %d attempts over %d ms:\n%s"
                    c/*host* attempt waited (failure-message failure))))))))

(def os
  "Ubuntu, with apt made to wait for its own locks and for an archive that is
  busy, and to give up on one that has stopped answering."
  (reify os/OS
    (setup! [_ test node]
      (info node "configuring apt to wait for its locks and to time out on the archive")
      (stop-apt-boot-jobs!)
      (c/su (cu/write-file! apt-conf-content apt-conf-path))
      (assert-apt-options!)
      (ensure-package-lists!)
      (setup-packages! test node)
      ;; Recorded last, so the recorded fact is that a whole setup succeeded.
      (swap! apt-updated-hosts conj c/*host*)
      (rearm-archive-retries!))

    (teardown! [_ test node]
      ;; apt-conf-path stays: the rest of the tests in this run reuse the node.
      (os/teardown! ubuntu/os test node))))
