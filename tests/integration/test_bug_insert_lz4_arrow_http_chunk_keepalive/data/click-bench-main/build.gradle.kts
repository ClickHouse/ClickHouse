plugins {
    idea
    alias(libs.plugins.kotlinJvm)
    `java-test-fixtures`
    `jvm-test-suite`
}

idea {
    module {
        isDownloadJavadoc = true
        isDownloadSources = true
    }
}

repositories {
    gradlePluginPortal()
    mavenCentral()
}

dependencies {
    implementation(platform(libs.kotlinxCoroutinesBom))
    implementation(libs.kotlinxCoroutinesCore)

    implementation(platform(libs.http4kBom))
    implementation(libs.http4kFormatJackson)

    implementation(libs.okhttp)
    implementation(libs.urin)
    implementation(libs.result4k)

    implementation("com.fasterxml.uuid:java-uuid-generator:5.1.0")

    // Logging
    implementation(libs.slf4jApi)
    implementation(libs.kotlinxCoroutinesSlf4j)
    runtimeOnly("ch.qos.logback:logback-classic:1.5.18")
    runtimeOnly("net.logstash.logback:logstash-logback-encoder:7.4")

    // ClickHouse
    implementation("org.lz4:lz4-java:1.8.0")
    implementation(platform(libs.apacheArrowBom))
    implementation(libs.apacheArrowVector)
    implementation(libs.apacheArrowMemoryUnsafe)
    implementation(libs.apacheArrowCompression)

    implementation(libs.jooq)

    testFixturesImplementation(libs.okhttp)
    testFixturesImplementation(libs.urin)
    testFixturesImplementation(libs.result4k)
    testFixturesImplementation(platform(libs.kotlinxCoroutinesBom))
    testFixturesImplementation(libs.jooq)
    testFixturesImplementation(platform(libs.apacheArrowBom))
    testFixturesImplementation(libs.apacheArrowVector)
    testFixturesImplementation(libs.apacheArrowMemoryUnsafe)
    testFixturesImplementation(libs.apacheArrowCompression)
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}


fun TaskContainer.registerContainerLifecycleTasks(groupName: String, vararg podmanArguments: String) {
    fun containerExists(containerName: String): Boolean {
        val process = ProcessBuilder(
            "podman",
            "ps",
            "-a",
            "--filter",
            "name=$containerName",
            "--format",
            "{{.Names}}"
        ).start()
        return process.inputStream.bufferedReader().use { it.readText().trim() }.isNotEmpty()
    }

    fun containerIsRunning(containerName: String): Boolean {
        val process = ProcessBuilder(
            "podman",
            "ps",
            "--filter",
            "name=$containerName",
            "--filter",
            "status=running",
            "--format",
            "{{.Names}}"
        ).start()
        return process.inputStream.bufferedReader().use { it.readText().trim() }.isNotEmpty()
    }

    val containerName = "$groupName-container"

    register<Exec>("start${groupName.replaceFirstChar { it.uppercase() }}") {
        group = groupName
        doFirst {
            val containerIsRunning = containerIsRunning(containerName)
            if (!containerIsRunning && containerExists(containerName)) {
                logger.info("Starting $groupName")
                commandLine("podman", "start", containerName)
            } else if (!containerIsRunning) {
                logger.info("Running $groupName")
                commandLine(listOf("podman", "run", "--rm", "--detach", "--name", containerName) + podmanArguments)
            } else {
                logger.info("$groupName is already running")
                commandLine("echo", "this is just to make Gradle happy")
            }
        }
    }

    register<Exec>("stop${groupName.replaceFirstChar { it.uppercase() }}") {
        group = groupName
        commandLine("podman", "rm", "--force", containerName)
        doLast {
            logger.info("$groupName container has been stopped.")
        }
    }

}

tasks.registerContainerLifecycleTasks("clickhouse", "--publish", "18123:8123", "-v", "./clickhouse-local-config/users.xml:/etc/clickhouse-server/users.xml", "hub.docker.com/clickhouse/clickhouse-server:25.4@sha256:69ced3e6e017fb652d77a0496858806f89fc54653dad8c3c6893939aa05e9833")

tasks.register("startPodmanService", Exec::class) {
    group = "podman"
    workingDir = file("$projectDir/scripts")
    commandLine("bash", "./start_podman_service.sh")
}

tasks.register("stopPodmanService", Exec::class) {
    group = "podman"
    workingDir = file("$projectDir/scripts")
    commandLine("bash", "./stop_podman_service.sh")
}

tasks.register<JavaExec>("runClickhouseBench") {
    group = "application"
    description = "Run ClickhouseBenKt main class"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("org.foo.ClickhouseBenchKt")
    jvmArgs = listOf("--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED", "-Darrow.memory.debug.allocator=true")
}

