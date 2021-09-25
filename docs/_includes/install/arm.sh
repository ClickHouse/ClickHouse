# ARM (AArch64) build works on Amazon Graviton, Oracle Cloud, Huawei Cloud ARM machines.
# The support for AArch64 is pre-production ready.

wget 'https://builds.clickhouse.tech/master/aarch64/clickhouse'
chmod a+x ./clickhouse
sudo ./clickhouse install
