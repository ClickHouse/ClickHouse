// /// Concurrency tests for `ClusterFactory`'s `MultiVersion<ClustersSnapshot>` publish/read path.
// ///
// /// These tests are intentionally scoped to the snapshot machinery that is independent from
// /// `SQLClusterCatalog::initialize` (i.e. they do NOT touch Keeper). Specifically they exercise:
// ///   * `setCluster` / `removeCluster(name, source)` / `removeCluster(name)`
// ///   * `tryGetCluster` / `getClusters` / `hasCluster` / `getClustersVersion` /
// ///     `isClusterDefinedOnlyInRemoteServers`
// ///   * `applyClustersConfig` / `reloadClustersConfig`
// ///   * `prepareUpsert` / `prepareRemove` / `commitPrepared` (two-phase publish for DDL atomicity)
// ///
// /// Deadlock tests involving `SQLClusterCatalog::mutex` live in integration tests (they require
// /// a running Keeper instance).
// ///
// /// Run under ThreadSanitizer: a RelWithDebInfo+TSAN build of `unit_tests_dbms` is the intended
// /// environment. Optionally enable the thread fuzzer via THREAD_FUZZER_* env vars to amplify
// /// scheduling interleavings.
// ///
// ///     ./unit_tests_dbms --gtest_filter='ClusterFactoryRace.*'

// #include <Common/tests/gtest_global_context.h>
// #include <Interpreters/Cluster.h>
// #include <Interpreters/ClusterFactory.h>
// #include <Interpreters/Context.h>
// #include <Poco/AutoPtr.h>
// #include <Poco/Util/XMLConfiguration.h>

// #include <gtest/gtest.h>

// #include <atomic>
// #include <chrono>
// #include <memory>
// #include <sstream>
// #include <string>
// #include <thread>
// #include <vector>

// using namespace DB;

// namespace
// {

// /// Minimal `ClusterPtr` with no shards. Enough to drive the snapshot path — `setCluster` only
// /// touches `Cluster::setDefinitionMetadata` and stores the pointer in the builder.
// ClusterPtr makeDummyCluster(const String & name)
// {
//     const auto & settings = getContext().context->getSettingsRef();
//     std::vector<Cluster::ShardInitSpec> empty_specs;
//     return std::make_shared<Cluster>(settings, name, /*secret*/ String{}, std::move(empty_specs));
// }

// /// Build a small `<remote_servers>` config fragment suitable for `applyClustersConfig`.
// /// Use loopback so `Cluster::Address` construction resolves immediately (no slow DNS timeouts on
// /// bogus names). These tests never open connections to the configured replicas.
// ConfigurationPtr makeRemoteServersConfig(const std::vector<String> & cluster_names)
// {
//     std::ostringstream xml;
//     xml << "<clickhouse><remote_servers>";
//     for (const auto & name : cluster_names)
//     {
//         xml << "<" << name << "><shard><replica>"
//             << "<host>127.0.0.1</host><port>9000</port>"
//             << "</replica></shard></" << name << ">";
//     }
//     xml << "</remote_servers></clickhouse>";

//     std::istringstream istr(xml.str());
//     Poco::AutoPtr<Poco::Util::XMLConfiguration> cfg(new Poco::Util::XMLConfiguration(istr));
//     return ConfigurationPtr(cfg);
// }

// /// Remove any cluster left behind by a previous test case. `ClusterFactory::instance()` is a
// /// process-wide singleton, so we must clean up to keep tests independent.
// void cleanupClusters(const std::vector<String> & names)
// {
//     auto & factory = ClusterFactory::instance();
//     for (const auto & name : names)
//         factory.removeCluster(name);
// }

// }

// /// Scenario A: concurrent readers + writers on the SAME cluster name, each writer uses a FRESH
// /// `ClusterPtr`. Under TSan this should be race-free because every `setCluster` publishes a new
// /// `Cluster` object that no reader has ever observed.
// TEST(ClusterFactoryRace, SetClusterFreshPtr)
// {
//     auto & factory = ClusterFactory::instance();
//     const String name = "race_fresh";

//     constexpr size_t num_readers = 4;
//     constexpr auto duration = std::chrono::milliseconds(1000);
//     std::atomic<bool> stop{false};

//     std::vector<std::thread> readers;
//     for (size_t i = 0; i < num_readers; ++i)
//     {
//         readers.emplace_back([&factory, &stop, &name]
//         {
//             while (!stop.load(std::memory_order_relaxed))
//             {
//                 if (auto c = factory.tryGetCluster(name))
//                 {
//                     auto src = c->getDefinitionSource();
//                     ASSERT_TRUE(
//                         src == ClusterDefinitionSource::RemoteServersConfig
//                         || src == ClusterDefinitionSource::SQLCatalog
//                         || src == ClusterDefinitionSource::Discovery);
//                 }
//                 (void)factory.hasCluster(name);
//                 (void)factory.getClustersVersion();
//                 (void)factory.getClusters();
//             }
//         });
//     }

//     std::thread writer([&factory, &stop, &name]
//     {
//         while (!stop.load(std::memory_order_relaxed))
//             factory.setCluster(name, makeDummyCluster(name), ClusterDefinitionSource::Discovery);
//     });

//     std::this_thread::sleep_for(duration);
//     stop.store(true, std::memory_order_relaxed);
//     writer.join();
//     for (auto & t : readers)
//         t.join();

//     cleanupClusters({name});
// }

// /// Scenario A': SAME `ClusterPtr` reused across `setCluster` calls. `setCluster` internally calls
// /// `cluster->setDefinitionMetadata(source, 0)` which writes to the `Cluster` object. If any reader
// /// is simultaneously reading the same `Cluster` via a previously-published snapshot, this is a
// /// data race on `definition_source` / `definition_version`. TSan is expected to flag it.
// ///
// /// Enable this test only when deliberately hunting that race — it serves as documentation of the
// /// contract: once a `ClusterPtr` has been handed to `setCluster`, it must not be re-submitted.
// TEST(ClusterFactoryRace, DISABLED_SetClusterSamePtrKnownRace)
// {
//     auto & factory = ClusterFactory::instance();
//     const String name = "race_same_ptr";
//     auto shared = makeDummyCluster(name);

//     std::atomic<bool> stop{false};
//     std::thread reader([&factory, &stop, &name]
//     {
//         while (!stop.load(std::memory_order_relaxed))
//             if (auto c = factory.tryGetCluster(name))
//                 (void)c->getDefinitionSource();
//     });

//     for (size_t i = 0; i < 1000000; ++i)
//         factory.setCluster(name, shared, ClusterDefinitionSource::Discovery);

//     stop.store(true, std::memory_order_relaxed);
//     reader.join();
//     cleanupClusters({name});
// }

// /// Scenario B: source priority. A `SQLCatalog`/`Discovery`-sourced `removeCluster` must never
// /// evict a `RemoteServersConfig`-owned entry of the same name.
// TEST(ClusterFactoryRace, ConfigSourceWinsOverSQL)
// {
//     auto & factory = ClusterFactory::instance();
//     const String name = "race_priority";

//     auto cfg = makeRemoteServersConfig({name});
//     auto global_context = Context::getGlobalContextInstance();
//     ASSERT_TRUE(global_context);
//     factory.applyClustersConfig(
//         cfg,
//         global_context->getSettingsRef(),
//         global_context->getMacros(),
//         /*config_name*/ "remote_servers",
//         global_context);

//     std::atomic<bool> stop{false};
//     std::thread attacker([&factory, &stop, &name]
//     {
//         while (!stop.load(std::memory_order_relaxed))
//         {
//             factory.setCluster(name, makeDummyCluster(name), ClusterDefinitionSource::SQLCatalog);
//             factory.removeCluster(name, ClusterDefinitionSource::SQLCatalog);
//         }
//     });

//     const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
//     while (std::chrono::steady_clock::now() < deadline)
//     {
//         auto c = factory.tryGetCluster(name);
//         ASSERT_TRUE(c);
//         EXPECT_EQ(c->getDefinitionSource(), ClusterDefinitionSource::RemoteServersConfig);
//     }

//     stop.store(true, std::memory_order_relaxed);
//     attacker.join();

//     /// Config source still present after attack.
//     auto c = factory.tryGetCluster(name);
//     ASSERT_TRUE(c);
//     EXPECT_EQ(c->getDefinitionSource(), ClusterDefinitionSource::RemoteServersConfig);

//     cleanupClusters({name});
// }

// /// Scenario C: `applyClustersConfig` interleaved with dynamic `setCluster` / `removeCluster` on
// /// independent names. Asserts that `clusters_version` never decreases and that both families of
// /// entries remain visible.
// TEST(ClusterFactoryRace, ApplyConfigVsDynamicSource)
// {
//     auto & factory = ClusterFactory::instance();
//     const std::vector<String> config_names = {"race_cfg_a", "race_cfg_b"};
//     const String dyn_name = "race_discovery";

//     auto cfg = makeRemoteServersConfig(config_names);
//     auto global_context = Context::getGlobalContextInstance();
//     ASSERT_TRUE(global_context);

//     /// Pre-install the config so the main thread's `hasCluster` checks are not racing the applier's
//     /// very first `applyClustersConfig` call (otherwise the first iteration can observe an empty snapshot
//     /// that hasn't yet been populated with `race_cfg_a` / `race_cfg_b`).
//     factory.applyClustersConfig(
//         cfg,
//         global_context->getSettingsRef(),
//         global_context->getMacros(),
//         "remote_servers",
//         global_context);
//     for (const auto & n : config_names)
//         ASSERT_TRUE(factory.hasCluster(n)) << "initial apply did not install cluster: " << n;

//     std::atomic<bool> stop{false};
//     size_t last_version = factory.getClustersVersion();

//     std::thread applier([&]()
//     {
//         while (!stop.load(std::memory_order_relaxed))
//         {
//             factory.applyClustersConfig(
//                 cfg,
//                 global_context->getSettingsRef(),
//                 global_context->getMacros(),
//                 "remote_servers",
//                 global_context);
//         }
//     });

//     std::thread dynamic_writer([&]()
//     {
//         while (!stop.load(std::memory_order_relaxed))
//         {
//             factory.setCluster(dyn_name, makeDummyCluster(dyn_name), ClusterDefinitionSource::Discovery);
//             factory.removeCluster(dyn_name, ClusterDefinitionSource::Discovery);
//         }
//     });

//     const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(1000);
//     while (std::chrono::steady_clock::now() < deadline)
//     {
//         const size_t v = factory.getClustersVersion();
//         ASSERT_GE(v, last_version);
//         last_version = v;

//         /// Config clusters must stay visible across the whole run.
//         for (const auto & n : config_names)
//             ASSERT_TRUE(factory.hasCluster(n)) << "config cluster disappeared: " << n;
//     }

//     stop.store(true, std::memory_order_relaxed);
//     applier.join();
//     dynamic_writer.join();

//     cleanupClusters(config_names);
//     cleanupClusters({dyn_name});
// }

// /// Scenario D: `reloadClustersConfig` re-entrancy. Between the two snapshot grabs inside
// /// `reloadClustersConfig` another thread may install a new config via `applyClustersConfig` —
// /// the reload must detect that (`pinned_config != current->clusters_config`) and bail out
// /// without overwriting the newer state with stale rebuild results.
// TEST(ClusterFactoryRace, ReloadVsApplyConfig)
// {
//     auto & factory = ClusterFactory::instance();
//     const std::vector<String> names_a = {"race_reload_a"};
//     const std::vector<String> names_b = {"race_reload_b"};

//     auto cfg_a = makeRemoteServersConfig(names_a);
//     auto cfg_b = makeRemoteServersConfig(names_b);
//     auto global_context = Context::getGlobalContextInstance();
//     ASSERT_TRUE(global_context);

//     factory.applyClustersConfig(
//         cfg_a,
//         global_context->getSettingsRef(),
//         global_context->getMacros(),
//         "remote_servers",
//         global_context);

//     std::atomic<bool> stop{false};

//     /// Each flipper iteration: read the current selector, apply the corresponding config,
//     /// then store a selector describing WHAT WAS JUST APPLIED — so after join the final value
//     /// of `last_applied_b` unambiguously identifies the last config installed.
//     std::atomic<bool> last_applied_b{false}; /// false -> cfg_a (initial state)
//     std::thread flipper([&]()
//     {
//         bool next = true; /// alternate a/b; start with b because initial apply above was a
//         while (!stop.load(std::memory_order_relaxed))
//         {
//             factory.applyClustersConfig(
//                 next ? cfg_b : cfg_a,
//                 global_context->getSettingsRef(),
//                 global_context->getMacros(),
//                 "remote_servers",
//                 global_context);
//             last_applied_b.store(next, std::memory_order_relaxed);
//             next = !next;
//         }
//     });

//     std::thread reloader([&]()
//     {
//         while (!stop.load(std::memory_order_relaxed))
//             factory.reloadClustersConfig(global_context);
//     });

//     std::this_thread::sleep_for(std::chrono::milliseconds(2000));
//     stop.store(true, std::memory_order_relaxed);
//     flipper.join();
//     reloader.join();

//     /// Final state must match whichever config was applied last. After `flipper.join()` the
//     /// `last_applied_b` store from the final iteration is fully visible.
//     const bool b_final = last_applied_b.load(std::memory_order_relaxed);
//     const auto & present = b_final ? names_b : names_a;
//     const auto & absent = b_final ? names_a : names_b;
//     for (const auto & n : present)
//         EXPECT_TRUE(factory.hasCluster(n)) << n;
//     for (const auto & n : absent)
//         EXPECT_FALSE(factory.hasCluster(n)) << n;

//     cleanupClusters(names_a);
//     cleanupClusters(names_b);
// }

// /// Scenario F: readers vs. `shutdown()`-style empty-snapshot publish. We simulate shutdown by
// /// publishing an empty snapshot through `removeCluster(name)` for every known name while readers
// /// hit the factory. Full `ClusterFactory::shutdown()` is not called here because it tears down
// /// process-global state that other tests in the same binary rely on.
// TEST(ClusterFactoryRace, ReadersVsClear)
// {
//     auto & factory = ClusterFactory::instance();
//     const String name = "race_clear";

//     std::atomic<bool> stop{false};
//     std::atomic<uint64_t> reader_hits{0};

//     std::thread reader([&]()
//     {
//         while (!stop.load(std::memory_order_relaxed))
//         {
//             (void)factory.tryGetCluster(name);
//             (void)factory.getClusters();
//             reader_hits.fetch_add(1, std::memory_order_relaxed);
//         }
//     });

//     for (size_t i = 0; i < 5000; ++i)
//     {
//         factory.setCluster(name, makeDummyCluster(name), ClusterDefinitionSource::Discovery);
//         factory.removeCluster(name);
//     }

//     stop.store(true, std::memory_order_relaxed);
//     reader.join();

//     EXPECT_GT(reader_hits.load(), 0u);
//     cleanupClusters({name});
// }

// /// Scenario G: concurrent `setCluster` on DISJOINT cluster names (independent writers). This is the
// /// snapshot-layer analogue of "concurrently create different clusters": the copy-on-write publish
// /// path must merge all per-thread inserts without losing any name. Readers run in parallel so that
// /// any torn / missing publish also shows up as an invalid observed source.
// TEST(ClusterFactoryRace, SetClusterConcurrentDistinctNames)
// {
//     auto & factory = ClusterFactory::instance();

//     constexpr size_t num_writers = 4;
//     constexpr size_t per_writer_ops = 2000;
//     constexpr size_t num_readers = 2;

//     std::vector<String> all_names;
//     all_names.reserve(num_writers * per_writer_ops);
//     for (size_t w = 0; w < num_writers; ++w)
//         for (size_t k = 0; k < per_writer_ops; ++k)
//             all_names.push_back("race_create_" + std::to_string(w) + "_" + std::to_string(k));

//     std::atomic<bool> stop{false};

//     std::vector<std::thread> readers;
//     for (size_t r = 0; r < num_readers; ++r)
//     {
//         readers.emplace_back([&factory, &stop, &all_names]
//         {
//             while (!stop.load(std::memory_order_relaxed))
//             {
//                 for (const auto & n : all_names)
//                 {
//                     if (auto c = factory.tryGetCluster(n))
//                     {
//                         ASSERT_EQ(c->getName(), n)
//                             << "tryGetCluster returned mismatching cluster object";
//                         const auto src = c->getDefinitionSource();
//                         ASSERT_EQ(src, ClusterDefinitionSource::SQLCatalog)
//                             << "unexpected source for cluster " << n;
//                     }
//                 }
//                 (void)factory.getClustersVersion();
//             }
//         });
//     }

//     std::vector<std::thread> writers;
//     for (size_t w = 0; w < num_writers; ++w)
//     {
//         writers.emplace_back([&factory, w]
//         {
//             for (size_t k = 0; k < per_writer_ops; ++k)
//             {
//                 const String n = "race_create_" + std::to_string(w) + "_" + std::to_string(k);
//                 factory.setCluster(n, makeDummyCluster(n), ClusterDefinitionSource::SQLCatalog);
//             }
//         });
//     }

//     for (auto & t : writers)
//         t.join();
//     stop.store(true, std::memory_order_relaxed);
//     for (auto & t : readers)
//         t.join();

//     /// Every distinct name inserted by some writer must be visible in the final snapshot.
//     for (const auto & n : all_names)
//         EXPECT_TRUE(factory.hasCluster(n)) << n;

//     cleanupClusters(all_names);
// }

// /// Scenario H: concurrent `removeCluster` on DISJOINT cluster names (independent droppers). The
// /// snapshot-layer analogue of "concurrently drop different clusters": after all droppers finish,
// /// none of the target names may remain in the unified registry.
// TEST(ClusterFactoryRace, RemoveClusterConcurrentDistinctNames)
// {
//     auto & factory = ClusterFactory::instance();

//     constexpr size_t num_droppers = 4;
//     constexpr size_t per_dropper_ops = 2000;
//     constexpr size_t num_readers = 2;

//     std::vector<String> all_names;
//     all_names.reserve(num_droppers * per_dropper_ops);
//     for (size_t w = 0; w < num_droppers; ++w)
//         for (size_t k = 0; k < per_dropper_ops; ++k)
//             all_names.push_back("race_drop_" + std::to_string(w) + "_" + std::to_string(k));

//     /// Pre-populate the registry so the droppers have something to erase.
//     for (const auto & n : all_names)
//         factory.setCluster(n, makeDummyCluster(n), ClusterDefinitionSource::Discovery);
//     for (const auto & n : all_names)
//         ASSERT_TRUE(factory.hasCluster(n)) << n;

//     std::atomic<bool> stop{false};

//     std::vector<std::thread> readers;
//     for (size_t r = 0; r < num_readers; ++r)
//     {
//         readers.emplace_back([&factory, &stop, &all_names]
//         {
//             while (!stop.load(std::memory_order_relaxed))
//             {
//                 for (const auto & n : all_names)
//                     (void)factory.tryGetCluster(n);
//                 (void)factory.getClustersVersion();
//             }
//         });
//     }

//     std::vector<std::thread> droppers;
//     for (size_t w = 0; w < num_droppers; ++w)
//     {
//         droppers.emplace_back([&factory, w]
//         {
//             for (size_t k = 0; k < per_dropper_ops; ++k)
//             {
//                 const String n = "race_drop_" + std::to_string(w) + "_" + std::to_string(k);
//                 factory.removeCluster(n, ClusterDefinitionSource::Discovery);
//             }
//         });
//     }

//     for (auto & t : droppers)
//         t.join();
//     stop.store(true, std::memory_order_relaxed);
//     for (auto & t : readers)
//         t.join();

//     for (const auto & n : all_names)
//         EXPECT_FALSE(factory.hasCluster(n)) << n;

//     cleanupClusters(all_names);
// }

// /// --- `PreparedPublish` / `prepareUpsert` / `prepareRemove` / `commitPrepared` ---------------------------
// ///
// /// These tests pin down the "two-phase publish" contract that shields DDL callers from the "persistent
// /// state was committed but the local snapshot refresh threw" window. The rules under test:
// ///   1. A successful `commitPrepared(prepareUpsert(...))` installs the new cluster; the snapshot identity
// ///      changes atomically.
// ///   2. `prepareUpsert` without a matching `commitPrepared` is a no-op: dropping the handle leaves the
// ///      factory's snapshot untouched.
// ///   3. `prepareRemove` mirrors (2) for eviction.
// ///   4. Source priority (`RemoteServersConfig` wins) is enforced at prepare time: a same-name SQL upsert
// ///      returns an empty handle, and committing an empty handle is a successful no-op.
// ///   5. Optimistic CAS: if another writer publishes between prepare and commit, `commitPrepared` returns
// ///      `false`; a fresh `prepareUpsert` then commits successfully on the new base.
// ///   6. Concurrent committers under retry loops produce a consistent final state — every name that was
// ///      prepared for upsert (and not concurrently removed) is present in the final snapshot.

// TEST(ClusterFactoryPreparedPublish, UpsertCommitInstallsCluster)
// {
//     auto & factory = ClusterFactory::instance();
//     const String name = "prepared_upsert_commit";
//     cleanupClusters({name});

//     const auto version_before = factory.getClustersVersion();
//     ASSERT_FALSE(factory.hasCluster(name));

//     auto prepared = factory.prepareUpsert(name, makeDummyCluster(name), ClusterDefinitionSource::Discovery);
//     ASSERT_FALSE(prepared.empty()) << "non-conflicting upsert must return a non-empty PreparedPublish";

//     /// The prepare is a no-op until commit: factory state is still pre-prepare.
//     ASSERT_FALSE(factory.hasCluster(name)) << "prepare must not publish";

//     ASSERT_TRUE(factory.commitPrepared(std::move(prepared)));
//     ASSERT_TRUE(factory.hasCluster(name)) << name;

//     auto fetched = factory.tryGetCluster(name);
//     ASSERT_NE(fetched, nullptr) << name;
//     EXPECT_EQ(fetched->getName(), name);
//     EXPECT_EQ(fetched->getDefinitionSource(), ClusterDefinitionSource::Discovery);

//     /// `<remote_servers>` version must not move for a non-config mutation (consistent with `setCluster`).
//     EXPECT_EQ(factory.getClustersVersion(), version_before);

//     cleanupClusters({name});
// }

// TEST(ClusterFactoryPreparedPublish, PrepareUpsertDroppedWithoutCommitIsNoop)
// {
//     auto & factory = ClusterFactory::instance();
//     const String name = "prepared_upsert_dropped";
//     cleanupClusters({name});

//     {
//         auto prepared = factory.prepareUpsert(name, makeDummyCluster(name), ClusterDefinitionSource::Discovery);
//         ASSERT_FALSE(prepared.empty());
//         /// Intentionally do not commit: simulate "Keeper write failed between prepare and commit, so the
//         /// caller threw out the prepared snapshot". The destructor of `prepared` must not touch the factory.
//     }

//     EXPECT_FALSE(factory.hasCluster(name)) << "uncommitted prepare must not leak into the snapshot";

//     cleanupClusters({name});
// }

// TEST(ClusterFactoryPreparedPublish, PrepareRemoveDroppedWithoutCommitIsNoop)
// {
//     auto & factory = ClusterFactory::instance();
//     const String name = "prepared_remove_dropped";
//     cleanupClusters({name});

//     factory.setCluster(name, makeDummyCluster(name), ClusterDefinitionSource::Discovery);
//     ASSERT_TRUE(factory.hasCluster(name));

//     {
//         auto prepared = factory.prepareRemove(name, ClusterDefinitionSource::Discovery);
//         ASSERT_FALSE(prepared.empty());
//         /// Drop without committing — prepared eviction must not be observable.
//     }

//     EXPECT_TRUE(factory.hasCluster(name)) << "uncommitted prepareRemove must not evict";

//     cleanupClusters({name});
// }

// TEST(ClusterFactoryPreparedPublish, RemoteServersConfigWinsOverSqlPrepare)
// {
//     auto & factory = ClusterFactory::instance();
//     const String name = "prepared_priority_guard";
//     cleanupClusters({name});

//     auto global_context = Context::getGlobalContextInstance();
//     ASSERT_TRUE(global_context);

//     factory.applyClustersConfig(
//         makeRemoteServersConfig({name}),
//         global_context->getSettingsRef(),
//         global_context->getMacros(),
//         "remote_servers",
//         global_context);
//     ASSERT_TRUE(factory.hasCluster(name));

//     auto prepared = factory.prepareUpsert(name, makeDummyCluster(name), ClusterDefinitionSource::SQLCatalog);
//     EXPECT_TRUE(prepared.empty()) << "SQL source must not override a config-owned name at prepare time";

//     /// Commit of an empty handle is a successful no-op — callers commit unconditionally after prepare.
//     EXPECT_TRUE(factory.commitPrepared(std::move(prepared)));

//     /// Config ownership is preserved.
//     auto fetched = factory.tryGetCluster(name);
//     ASSERT_NE(fetched, nullptr);
//     EXPECT_EQ(fetched->getDefinitionSource(), ClusterDefinitionSource::RemoteServersConfig);

//     factory.applyClustersConfig(
//         makeRemoteServersConfig({}),
//         global_context->getSettingsRef(),
//         global_context->getMacros(),
//         "remote_servers",
//         global_context);
//     cleanupClusters({name});
// }

// TEST(ClusterFactoryPreparedPublish, CommitFailsWhenBaseMovedByConcurrentWriter)
// {
//     auto & factory = ClusterFactory::instance();
//     const String name_a = "prepared_cas_a";
//     const String name_b = "prepared_cas_b";
//     cleanupClusters({name_a, name_b});

//     /// Prepare two independent upserts against the SAME base snapshot: both see an empty factory.
//     auto prepared_a = factory.prepareUpsert(name_a, makeDummyCluster(name_a), ClusterDefinitionSource::Discovery);
//     auto prepared_b = factory.prepareUpsert(name_b, makeDummyCluster(name_b), ClusterDefinitionSource::Discovery);
//     ASSERT_FALSE(prepared_a.empty());
//     ASSERT_FALSE(prepared_b.empty());

//     ASSERT_TRUE(factory.commitPrepared(std::move(prepared_a)));
//     ASSERT_TRUE(factory.hasCluster(name_a));

//     /// `prepared_b` was built from a now-stale base (pre-`name_a`); committing it would silently drop
//     /// `name_a`. The CAS must reject the commit and leave the factory state intact.
//     EXPECT_FALSE(factory.commitPrepared(std::move(prepared_b)));
//     EXPECT_TRUE(factory.hasCluster(name_a)) << "failed CAS must not roll back the winning writer's state";
//     EXPECT_FALSE(factory.hasCluster(name_b)) << "failed CAS must not publish the stale pending snapshot";

//     /// Re-prepare on the new base and commit — this is the contract-prescribed retry.
//     auto retried = factory.prepareUpsert(name_b, makeDummyCluster(name_b), ClusterDefinitionSource::Discovery);
//     ASSERT_FALSE(retried.empty());
//     ASSERT_TRUE(factory.commitPrepared(std::move(retried)));
//     EXPECT_TRUE(factory.hasCluster(name_a));
//     EXPECT_TRUE(factory.hasCluster(name_b));

//     cleanupClusters({name_a, name_b});
// }

// TEST(ClusterFactoryPreparedPublish, CommitConsumesPreparedHandle)
// {
//     auto & factory = ClusterFactory::instance();
//     const String name = "prepared_consume_once";
//     cleanupClusters({name});

//     auto prepared = factory.prepareUpsert(name, makeDummyCluster(name), ClusterDefinitionSource::Discovery);
//     ASSERT_FALSE(prepared.empty());

//     ASSERT_TRUE(factory.commitPrepared(std::move(prepared)));
//     ASSERT_TRUE(factory.hasCluster(name));

//     /// After `std::move`, the moved-from handle must be empty; a second commit is a no-op success.
//     EXPECT_TRUE(prepared.empty());
//     EXPECT_TRUE(factory.commitPrepared(std::move(prepared)));

//     cleanupClusters({name});
// }

// /// Scenario I: many threads drive upserts for DISJOINT cluster names via the prepare/commit API with a
// /// CAS-failure retry loop (the contract-prescribed way to compose this primitive). After all writers
// /// finish, every prepared name must be visible. This also exercises the race between the CAS read of
// /// `clusters_state` and another thread's `clusters_state.set` — the test is expected to run under TSan.
// TEST(ClusterFactoryPreparedPublish, ConcurrentPrepareCommitDistinctNames)
// {
//     auto & factory = ClusterFactory::instance();

//     constexpr size_t num_writers = 4;
//     constexpr size_t per_writer_ops = 400;
//     constexpr size_t num_readers = 2;

//     std::vector<String> all_names;
//     all_names.reserve(num_writers * per_writer_ops);
//     for (size_t w = 0; w < num_writers; ++w)
//         for (size_t k = 0; k < per_writer_ops; ++k)
//             all_names.push_back("prep_race_" + std::to_string(w) + "_" + std::to_string(k));

//     std::atomic<bool> stop{false};
//     std::atomic<size_t> retries{0};

//     std::vector<std::thread> readers;
//     for (size_t r = 0; r < num_readers; ++r)
//     {
//         readers.emplace_back([&factory, &stop, &all_names]
//         {
//             while (!stop.load(std::memory_order_relaxed))
//             {
//                 for (const auto & n : all_names)
//                     (void)factory.tryGetCluster(n);
//                 (void)factory.getClustersVersion();
//             }
//         });
//     }

//     std::vector<std::thread> writers;
//     for (size_t w = 0; w < num_writers; ++w)
//     {
//         writers.emplace_back([&factory, &retries, w]
//         {
//             for (size_t k = 0; k < per_writer_ops; ++k)
//             {
//                 const String n = "prep_race_" + std::to_string(w) + "_" + std::to_string(k);
//                 while (true)
//                 {
//                     auto prepared = factory.prepareUpsert(n, makeDummyCluster(n), ClusterDefinitionSource::Discovery);
//                     if (prepared.empty())
//                         break;
//                     if (factory.commitPrepared(std::move(prepared)))
//                         break;
//                     retries.fetch_add(1, std::memory_order_relaxed);
//                 }
//             }
//         });
//     }

//     for (auto & t : writers)
//         t.join();
//     stop.store(true, std::memory_order_relaxed);
//     for (auto & t : readers)
//         t.join();

//     for (const auto & n : all_names)
//         EXPECT_TRUE(factory.hasCluster(n)) << n;

//     /// Sanity: with 4 writers racing for the same MultiVersion slot, CAS retries should be common. This
//     /// is not a correctness assertion (the test would still pass with zero retries if scheduling happens
//     /// to serialise writers), but a log line helps confirm the retry path is actually exercised.
//     testing::Test::RecordProperty("cas_retries", static_cast<int>(retries.load()));

//     cleanupClusters(all_names);
// }
