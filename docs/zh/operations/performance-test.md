---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 54
toc_title: "\u6D4B\u8BD5\u786C\u4EF6"
---

# 如何使用ClickHouse测试您的硬件 {#how-to-test-your-hardware-with-clickhouse}

使用此指令，您可以在任何服务器上运行基本的ClickHouse性能测试，而无需安装ClickHouse软件包。

1.  转到 “commits” 页数：https://github.com/ClickHouse/ClickHouse/commits/master

2.  点击第一个绿色复选标记或红色十字与绿色 “ClickHouse Build Check” 然后点击 “Details” 附近链接 “ClickHouse Build Check”. 在一些提交中没有这样的链接，例如与文档的提交。 在这种情况下，请选择具有此链接的最近提交。

3.  将链接复制到 “clickhouse” 二进制为amd64或aarch64.

4.  ssh到服务器并使用wget下载它:

<!-- -->

      # For amd64:
      wget https://clickhouse-builds.s3.yandex.net/0/00ba767f5d2a929394ea3be193b1f79074a1c4bc/1578163263_binary/clickhouse
      # For aarch64:
      wget https://clickhouse-builds.s3.yandex.net/0/00ba767f5d2a929394ea3be193b1f79074a1c4bc/1578161264_binary/clickhouse
      # Then do:
      chmod a+x clickhouse

1.  下载配置:

<!-- -->

      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/config.xml
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/users.xml
      mkdir config.d
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/config.d/path.xml -O config.d/path.xml
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/config.d/log_to_console.xml -O config.d/log_to_console.xml

1.  下载基准测试文件:

<!-- -->

      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/benchmark/clickhouse/benchmark-new.sh
      chmod a+x benchmark-new.sh
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/benchmark/clickhouse/queries.sql

1.  根据下载测试数据 [Yandex梅里卡数据集](../getting-started/example-datasets/metrica.md) 说明 (“hits” 表包含100万行）。

<!-- -->

      wget https://clickhouse-datasets.s3.yandex.net/hits/partitions/hits_100m_obfuscated_v1.tar.xz
      tar xvf hits_100m_obfuscated_v1.tar.xz -C .
      mv hits_100m_obfuscated_v1/* .

1.  运行服务器:

<!-- -->

      ./clickhouse server

1.  检查数据：ssh到另一个终端中的服务器

<!-- -->

      ./clickhouse client --query "SELECT count() FROM hits_100m_obfuscated"
      100000000

1.  编辑benchmark-new.sh，改变 `clickhouse-client` 到 `./clickhouse client` 并添加 `–-max_memory_usage 100000000000` 参数。

<!-- -->

      mcedit benchmark-new.sh

1.  运行基准测试:

<!-- -->

      ./benchmark-new.sh hits_100m_obfuscated

1.  将有关硬件配置的编号和信息发送到clickhouse-feedback@yandex-team.com

所有结果都在这里公布：https://clickhouse.技术/基准/硬件/
