In our fork of librdkafka we have a few patches that are not yet merged to upstream and we might also have to do some thing differently than in upstream librdkafka. For this reasons here are the steps we did to upgrade to new librdkafka versions.

# 2.8.0 -> 2.14.1

## Fixes to apply

- pthread_set_name_np on freebsd - https://github.com/confluentinc/librdkafka/pull/4982 (no CH PR)
- Do not set _POSIX_C_SOURCE for FreeBSD (makes clang-15 happy) - https://github.com/confluentinc/librdkafka/pull/4157 (no CH PR)
- Race in rd_kafka_fetch_pos2str - https://github.com/confluentinc/librdkafka/pull/4788 (no CH PR)
- Fix data race in timers - https://github.com/confluentinc/librdkafka/pull/5089 (https://github.com/ClickHouse/librdkafka/pull/13) - Merged upstream, but not yet included in 2.14.1
- Fix possible data-race for statistics - https://github.com/confluentinc/librdkafka/pull/4630 (https://github.com/ClickHouse/librdkafka/pull/11)
- Fix data race in rd_kafka_broker_fetch_toppars - https://github.com/confluentinc/librdkafka/pull/5266 (https://github.com/ClickHouse/librdkafka/pull/14)
- Fix lock-order-inversion in queue refcount operations - https://github.com/confluentinc/librdkafka/pull/5445 (https://github.com/ClickHouse/librdkafka/pull/15)
- Fix lock-order-inversion in rd_kafka_q_concat0 and rd_kafka_q_prepend0 - https://github.com/confluentinc/librdkafka/pull/5446 (https://github.com/ClickHouse/librdkafka/pull/16)

## ClickHouse only fixes

- Prefix librdkafka cJSON functions with `kafka_` prefix to separate them from AWS cJSON functions https://github.com/ClickHouse/librdkafka/commit/385e2bd22e90acc1fb14dccab03d55f397830e86 (originally https://github.com/ClickHouse/librdkafka/commit/2a25367b877e73380faab73dad5e5a0806d36b7a). Use regex to replace ` cJSON_([A-Za-z]+)\(` to `kafka_cJSON_$1(`

```
git remote add confluentinc https://github.com/confluentinc/librdkafka.git

git fetch confluentinc v2.14.1

git checkout -b ClickHouse/release-2.14.1 v2.14.1

git fetch confluentinc refs/pull/4982/head
git cherry-pick bf3dd4fbb2f78e61723a51ff233dee429ee38f29 # pthread_set_name_np on freebsd

git fetch confluentinc refs/pull/4157/head
git cherry-pick 1e79eba2fda27ed69ff510e774b713af76ccc4d0 # Do not set _POSIX_C_SOURCE for FreeBSD (makes clang-15 happy)

git fetch confluentinc refs/pull/4788/head
git cherry-pick 0c449f610db0e0a255b0e110775f2a3c044c37e7 # Race in rd_kafka_fetch_pos2str

git fetch confluentinc refs/pull/5089/head
git cherry-pick 5c185854404abf506d520042f61818d93d96cc91 # Fix data race in timers
git cherry-pick 46d17f637132c34c111880c9556abd13572c9f13

git fetch confluentinc refs/pull/4630/head
git cherry-pick ccc6962711709948759068852e0eb0b44a1c5eeb # Fix possible data-race for statistics

git fetch confluentinc refs/pull/5266/head
git cherry-pick 801a520ec2abf8baee54f769096763398085a6d7 # Fix data race in rd_kafka_broker_fetch_toppars

git cherry-pick 385e2bd22e90acc1fb14dccab03d55f397830e86 # Prefix librdkafka cJSON functions with `kafka_` prefix to separate them from AWS cJSON functions

git fetch confluentinc refs/pull/5445/head
git cherry-pick 7ff1c01d6ec573355215ba56f314a9ef20ea55e8 # Fix lock-order-inversion in queue refcount operations
git cherry-pick 180b5f13e4fe6e4a85941a281c6d2bde4ff3a627

git fetch confluentinc refs/pull/5446/head
git cherry-pick 336830d10f2e9804e024d9cf04d3d824fdce9aa9 # Fix lock-order-inversion in rd_kafka_q_concat0 and rd_kafka_q_prepend0
```

# 1.6.1 -> 2.8.0

Originally described [here](https://gist.github.com/filimonov/ad252aa601d4d99fb57d4d76f14aa2bf), but copied here future reference.

Listing the commits exist in https://github.com/ClickHouse/librdkafka/tree/39d4ed49ccf3406e2bf825d5d7b0903b5a290782 on top of upstream

```
git log upstream/master..39d4ed49ccf3406e2bf825d5d7b0903b5a290782  > ../rdkafka_log.txt
```

https://github.com/confluentinc/librdkafka/compare/master...ClickHouse:librdkafka:39d4ed49ccf3406e2bf825d5d7b0903b5a290782


### New fixes for 2.8

* https://github.com/confluentinc/librdkafka/pull/4982
* https://github.com/confluentinc/librdkafka/pull/4788
* https://github.com/confluentinc/librdkafka/pull/5089
* https://github.com/confluentinc/librdkafka/pull/5266
* https://github.com/ClickHouse/librdkafka/pull/15 - Fix lock-order-inversion in queue refcount operations (use atomic refcount instead of mutex-protected)

### ClickHouse-specific fixes for 2.8 (cannot be upstreamed)

* Calling 'kafka_' prefixed versions of cJSON to avoid clashes with aws-c-common's version of cJSON:

    https://github.com/ClickHouse/ClickHouse/pull/94343

    When you upgrade librdkafka, please make sure to search for new or changed calls to cJSON and modify them accordingly.

### Fixes done earlier


#### Redone differently

* https://github.com/ClickHouse/librdkafka/commit/81b413cc1c2a33ad4e96df856b89184efbd6221c

  https://github.com/ClickHouse/ClickHouse/pull/76621/commits/4f663106eef1c2789b198c6cee10c704d50f34f5


#### not yet merged by the upstream (reapplied)


* commit https://github.com/ClickHouse/librdkafka/commit/e685f8c6149171302bf18be36342dddd92e7b3ae + merge https://github.com/ClickHouse/librdkafka/commit/39d4ed49ccf3406e2bf825d5d7b0903b5a290782

  https://github.com/confluentinc/librdkafka/pull/4630

* commit https://github.com/ClickHouse/librdkafka/commit/3d29dcf1fb2e51e15b31c0d0391891ebc355c0e1 + merge https://github.com/ClickHouse/librdkafka/commit/2d2aab6f5b79db1cfca15d7bf0dee75d00d82082

  https://github.com/confluentinc/librdkafka/pull/4718/ (or alternative https://github.com/confluentinc/librdkafka/pull/4604/ )

* commit https://github.com/ClickHouse/librdkafka/commit/3d3bf79bf11d7ecbf96a4b5fd8a59bc63a490833 + merge https://github.com/ClickHouse/librdkafka/commit/6f3b483426a8c8ec950e27e446bec175cf8b553f

  https://github.com/confluentinc/librdkafka/pull/4157


#### Patches already exists in the upstream 2.8

* commit https://github.com/ClickHouse/librdkafka/commit/ff32b4e9eeafd0b276f010ee969179e4e9e6d0b2 + merge commit https://github.com/ClickHouse/librdkafka/commit/8fa998984512927850490567bf048ac291ce24a8

   https://github.com/confluentinc/librdkafka/pull/4232/

* commit https://github.com/ClickHouse/librdkafka/commit/f5f098da282bae8a1ba924ca5ee737bd18ab2c37 + merge commit https://github.com/ClickHouse/librdkafka/commit/6062e711a919fb3b669b243b7dceabd045d0e4a2

  https://github.com/confluentinc/librdkafka/commit/bead2e4acc8f0723fa44d21451f85859d0da76e0
  https://github.com/confluentinc/librdkafka/pull/3786


* commit https://github.com/ClickHouse/librdkafka/commit/b8554f1682062c85ba519eb54ef2f90e02b812cb

  https://github.com/confluentinc/librdkafka/commit/51c49f6975dc8322815d8a95d20dc952e4b1c542
  https://github.com/confluentinc/librdkafka/pull/3285

* commit https://github.com/ClickHouse/librdkafka/commit/cb3a4a3fcb35ca88f6f8474e557df938eedf8254

  https://github.com/confluentinc/librdkafka/commit/c4d56949006cfdab0bb35b1135498d832a3439f1
  https://github.com/confluentinc/librdkafka/pull/3376


* commit https://github.com/ClickHouse/librdkafka/commit/ae0dd50ab6a0bfd0e8bf794e9c5e71bbe783985a + commit https://github.com/ClickHouse/librdkafka/commit/5bf9c8607820a73b9625c9d29d9cdaec22470b07

  https://github.com/confluentinc/librdkafka/commit/a11e48ef9c1f0b0d7a9188990443de2db00913b9
  https://github.com/confluentinc/librdkafka/pull/3179


* commit https://github.com/ClickHouse/librdkafka/commit/f68f261ceafb887824deb7848e1ff8d2eb33f956 (Port to BoringSSL)

  https://github.com/confluentinc/librdkafka/commit/4c51ce5bca44af3898e79a6f683bffc40170437a
  https://github.com/confluentinc/librdkafka/pull/4065

```shell
git remote add confluentinc https://github.com/confluentinc/librdkafka.git

git fetch confluentinc master

git checkout -b clickhouse_librdkafka_2.8.0 confluentinc/master

git fetch confluentinc refs/pull/4982/head
git cherry-pick bf3dd4fbb2f78e61723a51ff233dee429ee38f29 # pthread_set_name_np on freebsd

git fetch confluentinc refs/pull/4157/head
git cherry-pick 1e79eba2fda27ed69ff510e774b713af76ccc4d0 # Do not set _POSIX_C_SOURCE for FreeBSD (makes clang-15 happy)

git fetch confluentinc refs/pull/4788/head
git cherry-pick 0c449f610db0e0a255b0e110775f2a3c044c37e7 # Race in rd_kafka_fetch_pos2str

git fetch confluentinc refs/pull/4630/head
git cherry-pick ccc6962711709948759068852e0eb0b44a1c5eeb # Fix possible data-race for statistics

git fetch confluentinc refs/pull/4718/head
git cherry-pick f979784bd38ff8023bbac87aefdb9ea421ad7744 # Fix data race when a buffer queue is being reset instead of being initialized

git fetch confluentinc refs/pull/5089/head
git cherry-pick 5c185854404abf506d520042f61818d93d96cc91 # Fix data race in timers

git fetch confluentinc refs/pull/5266/head
git cherry-pick 801a520ec2abf8baee54f769096763398085a6d7 # Fix data race in rd_kafka_broker_fetch_toppars

# Now pick the commit with librdkafka modifications for cJSON referenced here: https://github.com/ClickHouse/ClickHouse/pull/94343
# Please check for new / changed calls to cJSON and adjust them accordingly
```