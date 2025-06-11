# Clickhouse Benchmark

Small Kotlin project for demonstrating the issue where clickhouse seems to close connection prematurely 
in some occasions when clients writing various batch sizes.

## How to run
0. install java (version 21)
1. Update `distributionUrl` in [`gradle/wrapper/gradle-wrapper.properties`](./gradle/wrapper/gradle-wrapper.properties) if behind firewall.
2. Update `repositories` block in [`build.gradle.kts`](./build.gradle.kts) & [`setting.gradle.kts`](./settings.gradle.kts) if behind firewall.
3. Update `docker image location` of `clickhouse` image in `registerContainerLifecycleTasks` [`build.gradle.kts`](./build.gradle.kts) if behind firewall.
4. Start local clickhouse server `./gradlew startClickhouse` (running clickhouse with Test containers on localhost:8123)
5. Run main program `./gradlew runClickhouseBench`
6. Stop local clickhouse server `./gradlew stopClickhouse` (optional)

## Observations
When writing to clickhouse with fixed batch size, the issue is not reproducible.
```kotlin
runTestFor(localClickhouse, localCredentials, databaseName, tableName, 4_000_000, IntRange(10_000, 10_000)) // e.g. fixed batch size of 10_000
``` 
Broken Pipe / error 400 is observed with various batch sizes.
```kotlin
runTestFor(localClickhouse, localCredentials, databaseName, tableName, 4_000_000, IntRange(5_000, 10_000)) // e.g. random batch size with range 5_000 to 10_000
``` 