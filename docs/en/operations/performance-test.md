---
sidebar_position: 54
sidebar_label: Testing Hardware
---

# How to Test Your Hardware with ClickHouse

You can run a basic ClickHouse performance test on any server without installation of ClickHouse packages.


## Automated Run

You can run the benchmark with a single script.

1. Download the script.
```
wget https://raw.githubusercontent.com/ClickHouse/ClickBench/main/hardware/hardware.sh
```

2. Run the script.
```
chmod a+x ./hardware.sh
./hardware.sh
```

3. Copy the output and send it to feedback@clickhouse.com

All the results are published here: https://clickhouse.com/benchmark/hardware/
