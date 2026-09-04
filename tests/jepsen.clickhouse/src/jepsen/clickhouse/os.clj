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
  that into a failure this can retry."
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

(def apt-lock-retry-interval-ms 5000)

(def apt-archive-retry-interval-ms 15000)

(def lists-lock-error
  "What apt prints when /var/lib/apt/lists/lock is held. DPkg::Lock::Timeout
  does not cover this lock, so it needs its own retry."
  "Could not get lock /var/lib/apt/lists/lock")

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

(defn stop-apt-timers!
  "Stops the timers that start automatic apt runs. Best effort: a node without
  systemd, or with these units absent, is fine. Stops the timers and never the
  services, so a package operation already in flight is left to finish."
  []
  (try+
   (c/su (c/exec :systemctl :stop :apt-daily.timer :apt-daily-upgrade.timer))
   (catch Object _
     (warn "Could not stop the apt-daily timers; continuing"))))

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
  "Was this apt-get killed by the deadline it was run under rather than
  finished? `timeout` reports 124 for the signal it sends, and 137 for the KILL
  it follows up with when the command does not take the first one."
  [{:keys [exit]}]
  (contains? #{124 137} exit))

(defn failure-message
  "What to report for a failed apt-get: apt's own message, or what became of it
  when it was stopped before it could print one."
  [{:keys [exit] :as failure}]
  (or (apt-error failure)
      (if (killed-by-deadline? failure)
        (str "apt-get was killed after " apt-update-timeout-seconds
             " s without finishing, exit status " exit)
        (str "apt-get exited with status " exit " and printed nothing"))))

(defn lists-lock-held?
  "Did this apt-get fail because another apt holds /var/lib/apt/lists/lock?"
  [failure]
  (boolean (when-let [err (apt-error failure)]
             (str/includes? err lists-lock-error))))

(defn archive-unavailable?
  "Is this failed apt-get a failure to download from the archive, rather than a
  rejection of what was asked for? Matched on the message, because apt reports
  both with exit status 100, and on the exit status when the deadline stopped
  it, which is a mirror that answered nothing at all."
  [failure]
  (or (killed-by-deadline? failure)
      (boolean (when-let [err (apt-error failure)]
                 (some #(str/includes? err %) archive-fetch-errors)))))

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
  that count rather than being fatal on its own."
  []
  (let [start (System/currentTimeMillis)
        lock-deadline (+ start (* 1000 apt-lock-timeout-seconds))
        swallowed-err
        (loop [attempt 1]
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
                (do (Thread/sleep (long apt-lock-retry-interval-ms))
                    (recur (inc attempt)))
                (let [waited (- (System/currentTimeMillis) start)]
                  (throw+ {:type ::apt-lists-lock-unavailable
                           :waited-ms waited
                           :err (apt-error failure)}
                          nil
                          "apt could not take /var/lib/apt/lists/lock after %d ms:\n%s"
                          waited (failure-message failure))))

              ;; An archive that could not be downloaded from, with nothing on
              ;; the node to install from either: worth another attempt while
              ;; the run still has time for one.
              (and (archive-unavailable? failure)
                   (zero? (readable-package-index-count))
                   (may-retry-archive?))
              (do (warn "apt could not fetch the package lists on" c/*host*
                        (str "(attempt " attempt "), retrying:\n")
                        (failure-message failure))
                  (Thread/sleep (long apt-archive-retry-interval-ms))
                  (recur (inc attempt)))

              :else
              (failure-message failure))))]
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
      (stop-apt-timers!)
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
