---
sidebar_position: 1
sidebar_label: 2024
---

# 2024 Changelog

### ClickHouse release v24.12.2.29-stable (e06743658c1) FIXME as compared to v24.12.1.1614-stable (a6d2b2e35b2)

#### Improvement
* Backported in [#73909](https://github.com/ClickHouse/ClickHouse/issues/73909): Add the `latest_fail_error_code_name` column to `system.mutations`. We need this column to introduce a new metric on stuck mutations and use it to build graphs of the errors encountered in the cloud as well as, optionally, adding a new less-noisy alert. [#72398](https://github.com/ClickHouse/ClickHouse/pull/72398) ([Miсhael Stetsyuk](https://github.com/mstetsyuk)).

#### Bug Fix (user-visible misbehavior in an official stable release)
* Backported in [#73322](https://github.com/ClickHouse/ClickHouse/issues/73322): Fix the situation when a user can't run `REVOKE ALL ON *.*` because of implicit grants in the target access entity. [#72872](https://github.com/ClickHouse/ClickHouse/pull/72872) ([pufit](https://github.com/pufit)).
* Backported in [#73764](https://github.com/ClickHouse/ClickHouse/issues/73764): Fix parsing enum glob followed by range one. Fixes [#73473](https://github.com/ClickHouse/ClickHouse/issues/73473). [#73569](https://github.com/ClickHouse/ClickHouse/pull/73569) ([Konstantin Bogdanov](https://github.com/thevar1able)).
* Backported in [#73819](https://github.com/ClickHouse/ClickHouse/issues/73819): Fixed parallel_replicas_for_non_replicated_merge_tree being ignored in subqueries for non-replicated tables. [#73584](https://github.com/ClickHouse/ClickHouse/pull/73584) ([Igor Nikonov](https://github.com/devcrafter)).
* Backported in [#73681](https://github.com/ClickHouse/ClickHouse/issues/73681): Do not interpret queries in `EXPLAIN SYNTAX` to avoid logical errors with incorrect processing stage for distributed queries. Fixes [#65205](https://github.com/ClickHouse/ClickHouse/issues/65205). [#73634](https://github.com/ClickHouse/ClickHouse/pull/73634) ([Dmitry Novik](https://github.com/novikd)).
* Backported in [#73733](https://github.com/ClickHouse/ClickHouse/issues/73733): Fix crash in LIMIT BY COLUMNS. [#73686](https://github.com/ClickHouse/ClickHouse/pull/73686) ([Raúl Marín](https://github.com/Algunenano)).
* Backported in [#73923](https://github.com/ClickHouse/ClickHouse/issues/73923): Fix [#73737](https://github.com/ClickHouse/ClickHouse/issues/73737). [#73775](https://github.com/ClickHouse/ClickHouse/pull/73775) ([zhanglistar](https://github.com/zhanglistar)).

#### NO CL ENTRY

* NO CL ENTRY:  'Revert "Backport [#72398](https://github.com/ClickHouse/ClickHouse/issues/72398) to 24.12: add the `latest_fail_error_code_name` column to `system.mutations`"'. [#73973](https://github.com/ClickHouse/ClickHouse/pull/73973) ([Miсhael Stetsyuk](https://github.com/mstetsyuk)).

#### NOT FOR CHANGELOG / INSIGNIFICANT

* Backported in [#73862](https://github.com/ClickHouse/ClickHouse/issues/73862): Fix use-after-free in loadPathPrefixMap on thread pool exception. [#72870](https://github.com/ClickHouse/ClickHouse/pull/72870) ([Michael Kolupaev](https://github.com/al13n321)).
* Backported in [#73695](https://github.com/ClickHouse/ClickHouse/issues/73695): Take amount of filtered into account for query_plan_join_swap_table. [#73534](https://github.com/ClickHouse/ClickHouse/pull/73534) ([Vladimir Cherkasov](https://github.com/vdimir)).
* Backported in [#73755](https://github.com/ClickHouse/ClickHouse/issues/73755): Update `test_storage_s3_queue/test.py::test_upgrade(2)`. [#73607](https://github.com/ClickHouse/ClickHouse/pull/73607) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Backported in [#73747](https://github.com/ClickHouse/ClickHouse/issues/73747): Fix test_storage_s3_queue/test.py::test_alter_settings. [#73632](https://github.com/ClickHouse/ClickHouse/pull/73632) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Backported in [#73945](https://github.com/ClickHouse/ClickHouse/issues/73945): Reverts ClickHouse/ClickHouse[#62944](https://github.com/ClickHouse/ClickHouse/issues/62944) (there was a crash in the CI because of this). [#73738](https://github.com/ClickHouse/ClickHouse/pull/73738) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Backported in [#73928](https://github.com/ClickHouse/ClickHouse/issues/73928): Revert [#70957](https://github.com/ClickHouse/ClickHouse/issues/70957). [#73875](https://github.com/ClickHouse/ClickHouse/pull/73875) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Backported in [#74002](https://github.com/ClickHouse/ClickHouse/issues/74002): Use minimum of memory limits for external aggregation/sort. [#73933](https://github.com/ClickHouse/ClickHouse/pull/73933) ([Mikhail Artemenko](https://github.com/Michicosun)).

