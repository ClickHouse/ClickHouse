---
title: 문제 해결
---

[//]: # "이 파일은 FAQ > 문제 해결에 포함됩니다"

* [설치](#troubleshooting-installation-errors)
* [서버 연결](#troubleshooting-accepts-no-connections)
* [쿼리 처리](#troubleshooting-does-not-process-queries)
* [쿼리 처리 효율성](#troubleshooting-too-slow)

<div id="troubleshooting-installation-errors">
  ## 설치
</div>

<div id="you-cannot-get-deb-packages-from-clickhouse-repository-with-apt-get">
  ### apt-get으로 ClickHouse 리포지토리에서 deb 패키지를 가져올 수 없는 경우
</div>

* 방화벽 설정을 확인하십시오.
* 어떤 이유로든 리포지토리에 액세스할 수 없는 경우, [설치 가이드](../getting-started/install.md) 문서에 설명된 대로 패키지를 다운로드한 후 `sudo dpkg -i <packages>` 명령을 사용해 수동으로 설치하십시오. `tzdata` 패키지도 필요합니다.

<div id="you-cannot-update-deb-packages-from-clickhouse-repository-with-apt-get">
  ### apt-get으로는 ClickHouse 리포지토리의 deb 패키지를 업데이트할 수 없습니다
</div>

* GPG key가 변경되면 이 문제가 발생할 수 있습니다.

리포지토리 구성을 업데이트하려면 [setup](../getting-started/install.md#setup-the-debian-repository) 페이지의 안내를 따르십시오.

<div id="you-get-different-warnings-with-apt-get-update">
  ### `apt-get update` 실행 시 서로 다른 경고가 표시될 수 있습니다
</div>

* 표시되는 전체 경고 메시지는 다음 중 하나입니다:

```bash
N: Skipping acquire of configured file 'main/binary-i386/Packages' as repository 'https://packages.clickhouse.com/deb stable InRelease' doesn't support architecture 'i386'
```

```bash
E: Failed to fetch https://packages.clickhouse.com/deb/dists/stable/main/binary-amd64/Packages.gz  File has unexpected size (30451 != 28154). Mirror sync in progress?
```

```text
E: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Origin' value from 'Artifactory' to 'ClickHouse'
E: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Label' value from 'Artifactory' to 'ClickHouse'
N: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Suite' value from 'stable' to ''
N: This must be accepted explicitly before updates for this repository can be applied. See apt-secure(8) manpage for details.
```

```bash
Err:11 https://packages.clickhouse.com/deb stable InRelease
  400  Bad Request [IP: 172.66.40.249 443]
```

위 문제를 해결하려면 다음 스크립트를 사용하세요:

```bash
sudo rm /var/lib/apt/lists/packages.clickhouse.com_* /var/lib/dpkg/arch /var/lib/apt/lists/partial/packages.clickhouse.com_*
sudo apt-get clean
sudo apt-get autoclean
```

<div id="you-cant-get-packages-with-yum-because-of-wrong-signature">
  ### 잘못된 서명으로 인해 yum으로 패키지를 가져올 수 없습니다
</div>

가능한 원인: 캐시가 잘못되었거나 2022-09에 GPG 키가 업데이트된 후 손상되었을 수 있습니다.

해결 방법은 yum의 캐시와 lib 디렉터리를 비우는 것입니다:

```bash
sudo find /var/lib/yum/repos/ /var/cache/yum/ -name 'clickhouse-*' -type d -exec rm -rf {} +
sudo rm -f /etc/yum.repos.d/clickhouse.repo
```

그런 다음 [설치 가이드](../getting-started/install.md#from-rpm-packages)를 따라 진행하십시오

<div id="you-cant-run-docker-container">
  ### Docker 컨테이너를 실행할 수 없습니다
</div>

간단한 `docker run clickhouse/clickhouse-server`를 실행하면, 다음과 유사한 스택 트레이스와 함께 크래시가 발생합니다:

```bash
$ docker run -it clickhouse/clickhouse-server
........
Poco::Exception. Code: 1000, e.code() = 0, System exception: cannot start thread, Stack trace (when copying this message, always include the lines below):

0. Poco::ThreadImpl::startImpl(Poco::SharedPtr<Poco::Runnable, Poco::ReferenceCounter, Poco::ReleasePolicy<Poco::Runnable>>) @ 0x00000000157c7b34
1. Poco::Thread::start(Poco::Runnable&) @ 0x00000000157c8a0e
2. BaseDaemon::initializeTerminationAndSignalProcessing() @ 0x000000000d267a14
3. BaseDaemon::initialize(Poco::Util::Application&) @ 0x000000000d2652cb
4. DB::Server::initialize(Poco::Util::Application&) @ 0x000000000d128b38
5. Poco::Util::Application::run() @ 0x000000001581cfda
6. DB::Server::run() @ 0x000000000d1288f0
7. Poco::Util::ServerApplication::run(int, char**) @ 0x0000000015825e27
8. mainEntryClickHouseServer(int, char**) @ 0x000000000d125b38
9. main @ 0x0000000007ea4eee
10. ? @ 0x00007f67ff946d90
11. ? @ 0x00007f67ff946e40
12. _start @ 0x00000000062e802e
 (version 24.10.1.2812 (official build))
```

원인은 버전이 `20.10.10`보다 낮은 구버전 docker daemon입니다. 해결 방법은 업그레이드하거나 `docker run [--privileged | --security-opt seccomp=unconfined]`를 실행하는 것입니다. 다만 후자의 방법은 보안상 위험이 있습니다.

<div id="troubleshooting-accepts-no-connections">
  ## 서버에 연결하기
</div>

가능한 원인:

* 서버가 실행 중이 아닙니다.
* 예기치 않거나 잘못된 구성 매개변수입니다.

<div id="server-is-not-running">
  ### 서버가 실행 중이 아닙니다
</div>

**서버가 실행 중인지 확인합니다**

명령어:

```bash
$ sudo service clickhouse-server status
```

서버가 실행되고 있지 않다면 다음 명령으로 시작하십시오:

```bash
$ sudo service clickhouse-server start
```

**로그 확인**

`clickhouse-server`의 기본 메인 로그는 기본적으로 `/var/log/clickhouse-server/clickhouse-server.log`에 있습니다.

서버가 성공적으로 시작되면 다음 문자열이 표시됩니다.

* `<Information> Application: starting up.` — 서버가 시작되었습니다.
* `<Information> Application: Ready for connections.` — 서버가 실행 중이며 연결을 받을 준비가 되었습니다.

`clickhouse-server`가 구성 오류로 시작에 실패한 경우에는 오류 설명과 함께 `<Error>` 문자열이 표시됩니다. 예시:

```text
2019.01.11 15:23:25.549505 [ 45 ] {} <Error> ExternalDictionaries: Failed reloading 'event2id' external dictionary: Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection refused, e.what() = Connection refused
```

파일 끝에 오류가 보이지 않으면 다음 문자열이 나오는 위치부터 파일 전체를 확인하십시오:

```text
<Information> Application: starting up.
```

서버에서 `clickhouse-server`의 두 번째 인스턴스를 시작하려고 하면 다음과 같은 로그가 표시됩니다:

```text
2019.01.11 15:25:11.151730 [ 1 ] {} <Information> : Starting ClickHouse 19.1.0 with revision 54413
2019.01.11 15:25:11.154578 [ 1 ] {} <Information> Application: starting up
2019.01.11 15:25:11.156361 [ 1 ] {} <Information> StatusFile: Status file ./status already exists - unclean restart. Contents:
PID: 8510
Started at: 2019-01-11 15:24:23
Revision: 54413

2019.01.11 15:25:11.156673 [ 1 ] {} <Error> Application: DB::Exception: Cannot lock file ./status. Another server instance in same directory is already running.
2019.01.11 15:25:11.156682 [ 1 ] {} <Information> Application: shutting down
2019.01.11 15:25:11.156686 [ 1 ] {} <Debug> Application: Uninitializing subsystem: Logging Subsystem
2019.01.11 15:25:11.156716 [ 2 ] {} <Information> BaseDaemon: Stop SignalListener thread
```

**system.d 로그 보기**

`clickhouse-server` 로그에서 유용한 정보를 찾을 수 없거나 로그 자체가 없는 경우, 다음 명령으로 `system.d` 로그를 확인할 수 있습니다:

```bash
$ sudo journalctl -u clickhouse-server
```

**clickhouse-server를 대화형 모드로 시작하기**

```bash
$ sudo -u clickhouse /usr/bin/clickhouse-server --config-file /etc/clickhouse-server/config.xml
```

이 명령은 자동 시작 스크립트의 기본 매개변수를 사용해 서버를 대화형 애플리케이션으로 시작합니다. 이 모드에서는 `clickhouse-server`가 모든 이벤트 메시지를 콘솔에 출력합니다.

<div id="configuration-parameters">
  ### 구성 매개변수
</div>

다음을 확인하십시오:

* Docker 설정.

  IPv6 네트워크의 Docker에서 ClickHouse를 실행하는 경우 `network=host`가 설정되어 있는지 확인하십시오.

* 엔드포인트 설정.

  [listen&#95;host](../operations/server-configuration-parameters/settings.md#listen_host) 및 [tcp&#95;port](../operations/server-configuration-parameters/settings.md#tcp_port) 설정을 확인하십시오.

  ClickHouse 서버는 기본적으로 localhost 연결만 허용합니다.

* HTTP 프로토콜 설정.

  HTTP API의 프로토콜 설정을 확인하십시오.

* 보안 연결 설정.

  다음을 확인하십시오:

  * [tcp&#95;port&#95;secure](../operations/server-configuration-parameters/settings.md#tcp_port_secure) 설정
  * [SSL certificates](../operations/server-configuration-parameters/settings.md#openssl) 설정

    연결할 때 올바른 매개변수를 사용하십시오. 예를 들어 `clickhouse_client`와 함께 `port_secure` 매개변수를 사용하십시오.

* 사용자 설정.

  사용자 이름이나 비밀번호가 잘못되었을 수 있습니다.

<div id="troubleshooting-does-not-process-queries">
  ## 쿼리 처리
</div>

ClickHouse가 쿼리를 처리하지 못하면 클라이언트에 오류 설명을 보냅니다. `clickhouse-client`에서는 콘솔에서 오류 설명을 확인할 수 있습니다. HTTP 인터페이스를 사용하는 경우 ClickHouse는 응답 본문에 오류 설명을 보냅니다. 예시:

```bash
$ curl 'http://localhost:8123/' --data-binary "SELECT a"
Code: 47, e.displayText() = DB::Exception: Unknown identifier: a. Note that there are no tables (FROM clause) in your query, context: required_names: 'a' source_tables: table_aliases: private_aliases: column_aliases: public_columns: 'a' masked_columns: array_join_columns: source_columns: , e.what() = DB::Exception
```

`clickhouse-client`를 `stack-trace` 매개변수와 함께 실행하면 ClickHouse가 오류 설명과 함께 서버 스택 트레이스를 반환합니다.

연결이 끊어졌다는 메시지가 표시될 수 있습니다. 이 경우 쿼리를 다시 실행할 수 있습니다. 쿼리를 실행할 때마다 연결이 끊어진다면 서버 로그에서 오류를 확인하십시오.

<div id="troubleshooting-too-slow">
  ## 쿼리 처리 효율성
</div>

ClickHouse가 너무 느리게 동작한다면, 쿼리에 대한 서버 리소스 및 네트워크 부하를 프로파일링해야 합니다.

쿼리를 프로파일링하려면 `clickhouse-benchmark` 유틸리티를 사용할 수 있습니다. 이 유틸리티는 초당 처리되는 쿼리 수, 초당 처리되는 행 수, 그리고 쿼리 처리 시간의 백분위수를 보여줍니다.