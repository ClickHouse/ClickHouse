---
sidebar_position: 54
sidebar_label: "\u6D4B\u8BD5\u786C\u4EF6"
---

# 如何使用 ClickHouse 测试您的硬件 {#how-to-test-your-hardware-with-clickhouse}

你可以在任何服务器上运行基本的 ClickHouse 性能测试，而无需安装 ClickHouse 软件包。


## 自动运行

你可以使用一个简单脚本来运行基准测试。

1. 下载脚本
```
wget https://raw.githubusercontent.com/ClickHouse/ClickBench/main/hardware/hardware.sh
```

2. 运行脚本
```
chmod a+x ./hardware.sh
./hardware.sh
```

3. 复制输出的信息并将它发送给 feedback@clickhouse.com 

所有的结果都在这里公布： https://clickhouse.com/benchmark/hardware/


## 人工运行

或者，你可以按照以下步骤实施基准测试。
```bash
# For amd64:
wget https://builds.clickhouse.com/master/amd64/clickhouse
# For aarch64:
wget https://builds.clickhouse.com/master/aarch64/clickhouse
# For powerpc64le:
wget https://builds.clickhouse.com/master/powerpc64le/clickhouse
# For freebsd:
wget https://builds.clickhouse.com/master/freebsd/clickhouse
# For freebsd-aarch64:
wget https://builds.clickhouse.com/master/freebsd-aarch64/clickhouse
# For freebsd-powerpc64le:
wget https://builds.clickhouse.com/master/freebsd-powerpc64le/clickhouse
# For macos:
wget https://builds.clickhouse.com/master/macos/clickhouse
# For macos-aarch64:
wget https://builds.clickhouse.com/master/macos-aarch64/clickhouse
# Then do:
chmod a+x clickhouse
```

2. 下载基准文件
```bash
wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/benchmark/hardware/benchmark-new.sh
chmod a+x benchmark-new.sh
wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/benchmark/hardware/queries.sql
```

3.  根据 [Yandex.Metrica 数据集](../getting-started/example-datasets/metrica.md) 中的说明下载测试数据（“ hits ” 数据表包含 1 亿行记录）。
```bash
wget https://datasets.clickhouse.com/hits/partitions/hits_100m_obfuscated_v1.tar.xz
tar xvf hits_100m_obfuscated_v1.tar.xz -C .
mv hits_100m_obfuscated_v1/* .
```

4. 运行服务器：
```bash
./clickhouse server
```

5. 检查数据：在另一个终端中通过 ssh 登陆服务器
```bash
./clickhouse client --query "SELECT count() FROM hits_100m_obfuscated"
100000000
```
6. 运行基准测试：
```bash
./benchmark-new.sh hits_100m_obfuscated
```

7. 将有关硬件配置的型号和信息发送到 clickhouse-feedback@yandex-team.com

所有结果都在这里公布：https://clickhouse.com/benchmark/hardware/
