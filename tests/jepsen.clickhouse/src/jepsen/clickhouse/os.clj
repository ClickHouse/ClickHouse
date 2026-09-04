(ns jepsen.clickhouse.os
  "Ubuntu OS setup that makes apt wait, rather than fail, for the two things
  that keep an installation from starting or finishing.

  apt's default lock timeout is 0, so an apt-get that finds a lock held exits
  immediately rather than waiting. A node is booted seconds before a test runs
  it, while the automatic apt jobs of a fresh boot still hold those locks, so
  jepsen.os.ubuntu/setup! needs apt configured to wait before it installs.

  The archive apt downloads the packages from also fails on its own: an EC2
  Ubuntu mirror rate-limits, answers 503, or drops the connection, and apt
  exits 100 having fetched nothing. Every test in a run sets its nodes up
  again, so a mirror unavailable for minutes crashes the tests that start
  inside that window and none of the rest, which is why the installation is
  retried here rather than left to fail the test."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.os :as os]
            [jepsen.os.debian :as debian]
            [jepsen.os.ubuntu :as ubuntu]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]
            [jepsen.util :as util]
            [clj-commons.slingshot :refer [try+ throw+]]))

(def apt-lock-conf-path
  "/etc/apt/apt.conf.d/99-clickhouse-jepsen-dpkg-lock-timeout")

(def apt-lock-timeout-seconds 600)

(def apt-lock-retry-interval-ms 5000)

(def apt-lock-conf-content
  (str "DPkg::Lock::Timeout \"" apt-lock-timeout-seconds "\";\n"))

(def lists-lock-error
  "What apt prints when /var/lib/apt/lists/lock is held. DPkg::Lock::Timeout
  does not cover this lock, so it needs its own retry."
  "Could not get lock /var/lib/apt/lists/lock")

(defn stop-apt-timers!
  "Stops the timers that start automatic apt runs. Best effort: a node without
  systemd, or with these units absent, is fine. Stops the timers and never the
  services, so a package operation already in flight is left to finish."
  []
  (try+
   (c/su (c/exec :systemctl :stop :apt-daily.timer :apt-daily-upgrade.timer))
   (catch Object _
     (warn "Could not stop the apt-daily timers; continuing"))))

(def apt-lock-timeout-dumped
  "The line `apt-config dump` prints for the timeout the drop-in sets. Matching
  it anchored is what distinguishes an effective drop-in from an inert one: apt
  always reports its own compiled-in Binary::apt::DPkg::Lock::Timeout, so an
  unanchored match succeeds even with no drop-in at all."
  (str "DPkg::Lock::Timeout \"" apt-lock-timeout-seconds "\";"))

(defn assert-apt-lock-timeout!
  "Asserts apt parses the drop-in and resolves the timeout to the value written.
  apt-config echoes back an unknown key too, so this establishes that apt reads
  the file, not that apt honours the option; that the timeout takes effect under
  contention is what the install measurement shows. Catches a malformed drop-in
  (apt-config then exits 100) and an inert one (no matching line). Takes no
  lock, so it cannot block."
  []
  (let [dumped (try+
                ;; As root: apt skips a config file it cannot read, with a warning and exit 0.
                (c/su (c/exec :apt-config :dump))
                (catch [:type :jepsen.control/nonzero-exit] {:keys [err]}
                  (throw+ {:type ::apt-conf-unusable
                           :err err}
                          nil
                          "apt cannot read its configuration, so %s does not parse:\n%s"
                          apt-lock-conf-path err)))]
    (when-not (some #{apt-lock-timeout-dumped} (str/split-lines dumped))
      (throw+ {:type ::apt-lock-timeout-not-in-effect}
              nil
              "apt does not report %s, so %s is not in effect"
              apt-lock-timeout-dumped apt-lock-conf-path))
    (info "apt reports" apt-lock-timeout-dumped)))

(def apt-updated-hosts
  "Hosts on which this process has completed a setup, so their package lists
  were fetched by an update we issued and saw succeed. Held in memory because
  the node carries no state we can trust to be from this run, and read before
  any apt call: apt's own caches are refreshed even by an update that failed to
  take a lock and fetched nothing."
  (atom #{}))

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

(defn ensure-package-lists!
  "Runs one apt-get update per host, retrying only while apt reports the lists
  lock held. DPkg::Lock::Timeout does not cover that lock. The update's exit
  status is the oracle in neither direction: apt exits 0 having fetched nothing
  when every source is unreachable, and exits non-zero having fetched every
  index that matters when one configured source is broken. The readable package
  indexes it leaves behind are what decides, so a non-lock failure stops the
  retrying and is then judged by that count rather than being fatal on its own."
  []
  (when-not (contains? @apt-updated-hosts c/*host*)
    (let [start (System/currentTimeMillis)
          deadline (+ start (* 1000 apt-lock-timeout-seconds))
          swallowed-err
          (loop []
            (let [[kind err]
                  (try+
                   ;; debian/update! hardcodes its argv, so its lock is taken here.
                   (util/with-named-lock debian/node-locks c/*host*
                     (c/su (c/exec :apt-get
                                   :--allow-releaseinfo-change :update)))
                   nil
                   (catch [:type :jepsen.control/nonzero-exit] {:keys [err]}
                     (if (and (string? err) (str/includes? err lists-lock-error))
                       [:lists-lock err]
                       [:other err])))]
              (case kind
                nil nil
                :other err
                :lists-lock
                (if (< (System/currentTimeMillis) deadline)
                  (do (Thread/sleep (long apt-lock-retry-interval-ms))
                      (recur))
                  (let [waited (- (System/currentTimeMillis) start)]
                    (throw+ {:type ::apt-lists-lock-unavailable
                             :waited-ms waited
                             :err err}
                            nil
                            "apt could not take /var/lib/apt/lists/lock after %d ms:\n%s"
                            waited err))))))]
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
              (- (System/currentTimeMillis) start) "ms," indexes "package indexes")))))

(def archive-fetch-errors
  "What apt prints when it could not download a package it resolved from a
  readable index. Nothing about the node or the request is wrong in that case,
  so these are worth retrying: the mirror is unavailable, overloaded, or
  unresolvable for now. `Unable to fetch some archives` is the summary apt adds
  to every one of them, so on its own it identifies the class; the per-item
  lines are listed too, so a message carrying only those still matches."
  ["Unable to fetch some archives"
   "Failed to fetch"
   "Could not resolve"
   "Temporary failure resolving"])

(def apt-install-timeout-seconds 600)

(def apt-install-retry-interval-ms 15000)

(defn archive-fetch-error?
  "Is this apt stderr a failure to download a package, rather than a rejection
  of what was asked for? Matched on the message because apt reports both with
  exit status 100."
  [err]
  (and (string? err)
       (boolean (some #(str/includes? err %) archive-fetch-errors))))

(defn setup-packages!
  "Runs the Ubuntu setup, retrying while apt cannot download a package. Retries
  the whole setup and not an install of our own listing of the packages, so the
  packages covered stay whatever the Ubuntu setup installs; each attempt asks
  apt only for what is still missing, so a retry resumes rather than restarts.
  Only an error identifying a download failure is retried - a package apt
  refuses to install is reported at once, as before. Leaves apt's own message
  in the exception it throws at the deadline, so a mirror that is broken rather
  than busy is diagnosed from it."
  [test node]
  (let [start (System/currentTimeMillis)
        deadline (+ start (* 1000 apt-install-timeout-seconds))]
    (loop [attempt 1]
      (when-let [err (try+
                      (os/setup! ubuntu/os test node)
                      nil
                      (catch [:type :jepsen.control/nonzero-exit] {:keys [err]}
                        (if (archive-fetch-error? err)
                          err
                          (throw+))))]
        (if (< (System/currentTimeMillis) deadline)
          (do (warn "apt could not download a package on" c/*host*
                    "(attempt" attempt "), retrying:\n" err)
              (Thread/sleep (long apt-install-retry-interval-ms))
              (recur (inc attempt)))
          (let [waited (- (System/currentTimeMillis) start)]
            (throw+ {:type ::apt-archives-unreachable
                     :node c/*host*
                     :attempts attempt
                     :waited-ms waited
                     :err err}
                    nil
                    "apt could not download a package on %s in %d attempts over %d ms:\n%s"
                    c/*host* attempt waited err)))))))

(def os
  "Ubuntu, with apt made to wait for its own locks, and for the archive it
  downloads the packages from, first."
  (reify os/OS
    (setup! [_ test node]
      (info node "configuring apt to wait for the dpkg locks")
      (stop-apt-timers!)
      (c/su (cu/write-file! apt-lock-conf-content apt-lock-conf-path))
      (assert-apt-lock-timeout!)
      (ensure-package-lists!)
      (setup-packages! test node)
      ;; Recorded last, so the recorded fact is that a whole setup succeeded.
      (swap! apt-updated-hosts conj c/*host*))

    (teardown! [_ test node]
      ;; apt-lock-conf-path stays: the rest of the tests in this run reuse the node.
      (os/teardown! ubuntu/os test node))))
