(ns jepsen.os-test
  "Drives jepsen.clickhouse.os/os against a fake node whose apt answers the way
  a degraded Ubuntu archive answers, so what the setup does about an archive
  that is busy, gone quiet, or serving an index it no longer matches is checked
  without a node and without an archive.

  The Ubuntu setup and the apt calls under it are the real ones: only the node
  is fake, as a jepsen Remote that answers each command out of a script. Run
  with `lein test :only jepsen.os-test`."
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen.os :as os]
            [jepsen.control :as c]
            [jepsen.control.core :as core]
            [jepsen.clickhouse.os :as chos]
            [clj-commons.slingshot :refer [try+]]))

;; The messages a real apt printed, from the CI logs of the two failures.

(def mirror-503
  (str "E: Failed to fetch http://us-east-1.ec2.archive.ubuntu.com/ubuntu/pool/universe/l/libfaketime/libfaketime_0.9.8-9_amd64.deb  Connection failed [IP: 34.228.244.102 80]\n"
       "E: Failed to fetch http://us-east-1.ec2.archive.ubuntu.com/ubuntu/pool/universe/l/libfaketime/faketime_0.9.8-9_amd64.deb  503  Service Unavailable [IP: 54.145.9.175 80]\n"
       "E: Unable to fetch some archives, maybe run apt-get update or try with --fix-missing?\n"))

(def mirror-404
  (str "E: Failed to fetch http://us-east-1.ec2.archive.ubuntu.com/ubuntu/pool/universe/l/libfaketime/faketime_0.9.8-9_amd64.deb  404  Not Found [IP: 34.228.244.102 80]\n"
       "E: Unable to fetch some archives, maybe run apt-get update or try with --fix-missing?\n"))

(def held-packages
  "E: Unable to correct problems, you have held broken packages.\n")

(defn lists-lock-held
  "What apt printed on the node the nightly run lost, where the `apt-get update`
  of the node's own boot held the lists lock for the whole job."
  ([] (lists-lock-held 1758 "apt-get"))
  ([pid holder]
   (str "E: Could not get lock /var/lib/apt/lists/lock. It is held by process "
        pid " (" holder ")\n"
        "E: Unable to lock directory /var/lib/apt/lists/\n")))

(def apt-config-defaults
  "The lines apt reports for its own compiled-in defaults for the options the
  drop-in sets, so a dump that carries only these is an inert drop-in."
  ["Binary::apt::DPkg::Lock::Timeout \"0\";"
   "Acquire::http::Timeout \"120\";"
   "Acquire::Retries \"0\";"
   "Dir \"/\";"])

;; A fake node. Each command is answered by the first matcher whose pattern the
;; command carries; the responses of the apt operations under test are scripted,
;; and every command is recorded so a test can count the attempts.

(defn fake-node
  "State of a fake node: the commands run on it, the files written to it, the
  package indexes it can read, and the scripted responses left for its apt-get
  update and apt-get install. A node starts with no readable index unless a
  test gives it one, and an update apt reports successful leaves it 22."
  [{:keys [updates installs indexes]}]
  (atom {:cmds []
         :files {}
         :updates (vec updates)
         :installs (vec installs)
         :indexes (or indexes 0)}))

(defn ok
  ([] (ok ""))
  ([out] {:exit 0 :out out :err ""}))

(defn next-response!
  "Takes the next scripted response for this kind of apt operation, keeping the
  last one once the script runs out."
  [node kind]
  (let [[response] (get @node kind)]
    (when (< 1 (count (get @node kind)))
      (swap! node update kind subvec 1))
    (or response (ok))))

(defn respond
  "How the fake node answers one command."
  [node {:keys [cmd in] :as action}]
  (cond
    (str/includes? cmd "cat > /etc/apt/apt.conf.d/")
    (do (swap! node assoc-in [:files "/etc/apt/apt.conf.d/99-clickhouse-jepsen"] in)
        (ok))

    (str/includes? cmd "apt-config dump")
    (ok (str/join "\n" (concat (str/split-lines
                                (get-in @node [:files "/etc/apt/apt.conf.d/99-clickhouse-jepsen"] ""))
                               apt-config-defaults)))

    (str/includes? cmd "apt-get indextargets")
    (ok (str/join "\n" (repeat (:indexes @node) "Created-By: Packages")))

    (str/includes? cmd "systemctl stop")
    (ok)

    ;; A signal reaches whatever the scripted update said held the lock: the
    ;; node has no processes, so what a kill does is only to be recorded.
    (str/includes? cmd "kill -")
    (ok)

    (str/includes? cmd "apt-get --allow-releaseinfo-change update")
    (let [response (next-response! node :updates)]
      (when (zero? (:exit response))
        (swap! node assoc :indexes 22))
      response)

    (str/includes? cmd "apt-get install")
    (next-response! node :installs)

    (str/includes? cmd "hostname")
    (ok "n1")

    (str/includes? cmd "cat /etc/hosts")
    (ok "127.0.0.1\tlocalhost")

    (str/includes? cmd "date +%s")
    (ok (str (quot (System/currentTimeMillis) 1000)))

    ;; Fresh: what jepsen.os.debian/maybe-update! reads to keep the lists we
    ;; fetched ourselves minutes ago.
    (str/includes? cmd "pkgcache.bin")
    (ok (str (quot (System/currentTimeMillis) 1000)))

    ;; Everything the Ubuntu setup wants installed is installed but these two,
    ;; which is what the CI logs show it asking for.
    (str/includes? cmd "dpkg --get-selections")
    (ok (->> (str/split cmd #"\s+")
             (drop-while #(not= % "--get-selections"))
             rest
             (remove #{"faketime" "ntpdate"})
             (map #(str (str/replace % "\"" "") "\tinstall"))
             (str/join "\n")))

    :else
    (do (info "fake node: unstubbed command" cmd)
        (ok))))

(defn remote
  "A jepsen Remote that answers out of a fake node."
  [node]
  (reify core/Remote
    (connect [this _] this)
    (disconnect! [_])
    (execute! [_ _ action]
      (swap! node update :cmds conj (:cmd action))
      (merge action (respond node action)))
    (upload! [_ _ _ _ _])
    (download! [_ _ _ _ _])))

(defn cmds-matching
  [node pattern]
  (filter #(str/includes? % pattern) (:cmds @node)))

(defn setup!
  "Runs the OS setup under test against a fake node, and returns the node."
  [node]
  (c/with-session "n1" (remote node)
    (binding [c/*remote* (remote node)]
      (os/setup! chos/os {:net nil} "n1")))
  node)

(defn setup-error
  "Runs the setup and returns what it threw, or nil."
  [node]
  (try+ (setup! node) nil
        (catch Object e e)))

(use-fixtures :each (fn [t]
                      (reset! chos/apt-updated-hosts #{})
                      (reset! chos/archive-retry-deadline nil)
                      (t)))

(deftest apt-options-are-written-and-asserted
  (let [node (setup! (fake-node {}))]
    (is (= (str "DPkg::Lock::Timeout \"600\";\n"
                "Acquire::http::Timeout \"30\";\n"
                "Acquire::https::Timeout \"30\";\n"
                "Acquire::Retries \"1\";\n")
           (get-in @node [:files "/etc/apt/apt.conf.d/99-clickhouse-jepsen"])))
    (is (= 1 (count (cmds-matching node "apt-config dump"))))))

(deftest an-inert-drop-in-is-rejected
  ;; apt reports its own defaults for these options, and nothing else.
  (with-redefs [chos/apt-conf-content "Nonsense::Option \"1\";\n"]
    (let [e (setup-error (fake-node {}))]
      (is (= :jepsen.clickhouse.os/apt-options-not-in-effect (:type e)))
      (is (= 4 (count (:missing e)))))))

(deftest the-update-runs-under-a-deadline
  (let [node (setup! (fake-node {}))]
    (is (= 1 (count (cmds-matching node "apt-get --allow-releaseinfo-change update"))))
    (is (str/includes? (first (cmds-matching node "apt-get --allow-releaseinfo-change update"))
                       "timeout --kill-after=10 300 apt-get"))))

(deftest an-install-that-cannot-download-is-retried
  (with-redefs [chos/apt-archive-retry-interval-ms 10]
    (let [node (setup! (fake-node {:installs [{:exit 100 :out "" :err mirror-503}
                                              {:exit 100 :out "" :err mirror-503}
                                              (ok)]}))]
      (is (= 3 (count (cmds-matching node "apt-get install"))))
      (is (= #{"n1"} @chos/apt-updated-hosts)))))

(deftest an-update-the-deadline-killed-is-retried
  (with-redefs [chos/apt-archive-retry-interval-ms 10]
    (let [node (setup! (fake-node {:updates [{:exit 124 :out "" :err ""}
                                             {:exit 124 :out "" :err ""}
                                             (ok)]}))]
      ;; The count of readable indexes is what judges an update, so it is read
      ;; per failed attempt as well as at the end.
      (is (= 3 (count (cmds-matching node "apt-get --allow-releaseinfo-change update"))))
      (is (= #{"n1"} @chos/apt-updated-hosts)))))

(deftest an-update-that-fetched-nothing-and-cannot-retry-is-fatal
  (with-redefs [chos/apt-archive-retry-interval-ms 10
                chos/apt-archive-retry-seconds 0]
    (let [e (setup-error (fake-node {:updates [{:exit 124 :out "" :err ""}]}))]
      (is (= :jepsen.clickhouse.os/apt-package-lists-empty (:type e)))
      (is (str/includes? (:err e) "apt-get update did not finish in 300 s")))))

(deftest an-update-that-failed-but-fetched-indexes-continues
  (let [node (setup! (fake-node {:updates [{:exit 100 :out "" :err mirror-503}]
                                 :indexes 22}))]
    ;; One attempt: the indexes it left behind are what the setup needs.
    (is (= 1 (count (cmds-matching node "apt-get --allow-releaseinfo-change update"))))
    (is (= 1 (count (cmds-matching node "apt-get install"))))))

(deftest a-package-the-lists-no-longer-name-refetches-them-once
  (with-redefs [chos/apt-archive-retry-interval-ms 10]
    (let [node (setup! (fake-node {:installs [{:exit 100 :out "" :err mirror-404}
                                              (ok)]}))
          updates (cmds-matching node "apt-get --allow-releaseinfo-change update")]
      (is (= 2 (count updates)))
      (is (= 2 (count (cmds-matching node "apt-get install"))))
      ;; Both updates are ours, under our deadline: the one inside the Ubuntu
      ;; setup keeps lists this young, which is why the retry has to fetch them.
      (is (every? #(str/includes? % "timeout --kill-after=10 300") updates)))))

(deftest a-package-missing-from-fresh-lists-is-reported-at-once
  (with-redefs [chos/apt-archive-retry-interval-ms 10]
    (let [node (fake-node {:installs [{:exit 100 :out "" :err mirror-404}]})
          start (System/currentTimeMillis)
          e (setup-error node)]
      (is (= :jepsen.clickhouse.os/apt-package-lists-stale (:type e)))
      (is (= 2 (:attempts e)))
      ;; Reported rather than retried: nothing of the retry window is spent.
      (is (> 60000 (- (System/currentTimeMillis) start)))
      (is (= 2 (count (cmds-matching node "apt-get install")))))))

(deftest an-install-killed-by-a-signal-is-not-an-unavailable-archive
  ;; Nothing but the update runs under a deadline, so 137 from an install is a
  ;; signal from elsewhere and there is no archive to wait for.
  (with-redefs [chos/apt-archive-retry-interval-ms 10]
    (let [node (fake-node {:installs [{:exit 137 :out "" :err ""}]})
          e (setup-error node)]
      (is (= :jepsen.control/nonzero-exit (:type e)))
      (is (= 1 (count (cmds-matching node "apt-get install")))))))

(deftest a-package-apt-refuses-to-install-is-reported-at-once
  (let [node (fake-node {:installs [{:exit 100 :out "" :err held-packages}]})
        e (setup-error node)]
    (is (= :jepsen.control/nonzero-exit (:type e)))
    (is (= 1 (count (cmds-matching node "apt-get install"))))))

(deftest an-unreachable-archive-is-reported-at-the-deadline
  (with-redefs [chos/apt-archive-retry-interval-ms 10
                chos/apt-archive-retry-seconds 1]
    (let [node (fake-node {:installs [{:exit 100 :out "" :err mirror-503}]})
          e (setup-error node)]
      (is (= :jepsen.clickhouse.os/apt-archives-unreachable (:type e)))
      (is (< 1 (:attempts e)))
      (is (str/includes? (:err e) "503")))))

(deftest the-retry-window-is-spent-once-by-the-whole-run
  (with-redefs [chos/apt-archive-retry-interval-ms 10
                chos/apt-archive-retry-seconds 1]
    ;; A first setup spends the window.
    (is (= :jepsen.clickhouse.os/apt-archives-unreachable
           (:type (setup-error (fake-node {:installs [{:exit 100 :out "" :err mirror-503}]})))))
    ;; A second one does not wait again: the window it finds is closed.
    (let [node (fake-node {:installs [{:exit 100 :out "" :err mirror-503}]})
          start (System/currentTimeMillis)]
      (is (= :jepsen.clickhouse.os/apt-archives-unreachable (:type (setup-error node))))
      (is (= 1 (:attempts (setup-error node))))
      (is (> 1000 (- (System/currentTimeMillis) start))))))

(deftest a-setup-that-completes-re-arms-a-spent-retry-window
  (with-redefs [chos/apt-archive-retry-interval-ms 10
                chos/apt-archive-retry-seconds 1]
    (is (= :jepsen.clickhouse.os/apt-archives-unreachable
           (:type (setup-error (fake-node {:installs [{:exit 100 :out "" :err mirror-503}]})))))
    (is (some? @chos/archive-retry-deadline))
    (setup! (fake-node {}))
    (is (nil? @chos/archive-retry-deadline))))

(deftest a-live-retry-window-is-left-to-the-other-nodes
  (with-redefs [chos/apt-archive-retry-interval-ms 10]
    (setup! (fake-node {:installs [{:exit 100 :out "" :err mirror-503}
                                   (ok)]}))
    ;; The window this setup opened still has time in it, and another node may
    ;; still be spending it.
    (is (some? @chos/archive-retry-deadline))))

(deftest the-boot-jobs-of-apt-are-stopped
  ;; The service as well as the timers: it is the one already running when a
  ;; node that booted seconds ago is set up.
  (let [node (setup! (fake-node {}))
        stops (cmds-matching node "systemctl stop")]
    (is (= 1 (count stops)))
    (is (every? #(str/includes? (first stops) %)
                ["apt-daily.timer" "apt-daily-upgrade.timer" "apt-daily.service"]))
    ;; The ones that run dpkg are waited for, not stopped.
    (is (not (str/includes? (first stops) "unattended-upgrades")))
    (is (not (str/includes? (first stops) "apt-daily-upgrade.service")))))

(deftest a-lists-lock-released-inside-the-grace-is-waited-for
  ;; What a healthy node does: the update of its own boot holds the lock for
  ;; seconds, so it is left to finish.
  (with-redefs [chos/apt-lock-retry-interval-ms 10]
    (let [node (setup! (fake-node {:updates [{:exit 100 :out "" :err (lists-lock-held)}
                                             {:exit 100 :out "" :err (lists-lock-held)}
                                             (ok)]}))]
      (is (= 3 (count (cmds-matching node "apt-get --allow-releaseinfo-change update"))))
      (is (empty? (cmds-matching node "kill -"))))))

(deftest a-lists-lock-holder-that-outlasts-its-grace-is-stopped
  ;; The node the nightly run lost: TERM once the grace is spent, KILL when the
  ;; next attempt finds the same process still there, and the update then runs.
  (with-redefs [chos/apt-lock-retry-interval-ms 10
                chos/apt-lists-lock-grace-seconds 0]
    (let [node (setup! (fake-node {:updates [{:exit 100 :out "" :err (lists-lock-held)}
                                             {:exit 100 :out "" :err (lists-lock-held)}
                                             (ok)]}))]
      (is (= ["kill -TERM 1758" "kill -KILL 1758"]
             (map #(str/replace % #".*(kill -\S+ \d+).*" "$1")
                  (cmds-matching node "kill -"))))
      (is (= 3 (count (cmds-matching node "apt-get --allow-releaseinfo-change update"))))
      (is (= #{"n1"} @chos/apt-updated-hosts)))))

(deftest a-lists-lock-holder-is-signalled-once-each-way
  ;; A holder that answers neither signal is waited for like any other, and
  ;; reported at the deadline rather than signalled again and again.
  (with-redefs [chos/apt-lock-retry-interval-ms 10
                chos/apt-lists-lock-grace-seconds 0
                chos/apt-lock-timeout-seconds 1]
    (let [node (fake-node {:updates [{:exit 100 :out "" :err (lists-lock-held)}]})
          e (setup-error node)]
      (is (= :jepsen.clickhouse.os/apt-lists-lock-unavailable (:type e)))
      (is (< 2 (count (cmds-matching node "apt-get --allow-releaseinfo-change update"))))
      (is (= 2 (count (cmds-matching node "kill -")))))))

(deftest a-new-lists-lock-holder-gets-its-own-signals
  (with-redefs [chos/apt-lock-retry-interval-ms 10
                chos/apt-lists-lock-grace-seconds 0]
    (let [node (setup! (fake-node {:updates [{:exit 100 :out "" :err (lists-lock-held 1758 "apt-get")}
                                             {:exit 100 :out "" :err (lists-lock-held 2001 "apt-get")}
                                             (ok)]}))]
      (is (= ["kill -TERM 1758" "kill -TERM 2001"]
             (map #(str/replace % #".*(kill -\S+ \d+).*" "$1")
                  (cmds-matching node "kill -")))))))

(deftest a-lists-lock-holder-that-is-not-an-apt-is-left-alone
  (with-redefs [chos/apt-lock-retry-interval-ms 10
                chos/apt-lists-lock-grace-seconds 0
                chos/apt-lock-timeout-seconds 1]
    (let [node (fake-node {:updates [{:exit 100 :out ""
                                      :err (lists-lock-held 1758 "packagekitd")}]})
          e (setup-error node)]
      (is (= :jepsen.clickhouse.os/apt-lists-lock-unavailable (:type e)))
      (is (empty? (cmds-matching node "kill -"))))))

(deftest a-lists-lock-that-is-never-released-is-reported
  (with-redefs [chos/apt-lock-retry-interval-ms 10
                chos/apt-lock-timeout-seconds 1]
    (let [e (setup-error (fake-node {:updates [{:exit 100 :out "" :err (lists-lock-held)}]}))]
      (is (= :jepsen.clickhouse.os/apt-lists-lock-unavailable (:type e)))
      (is (= "n1" (:node e)))
      (is (str/includes? (:err e) "Could not get lock")))))

(deftest a-host-with-a-completed-setup-is-not-updated-again
  (let [node (setup! (fake-node {}))]
    (setup! node)
    (is (= 1 (count (cmds-matching node "apt-get --allow-releaseinfo-change update"))))
    (is (= 2 (count (cmds-matching node "apt-config dump"))))))
