
# Download source code:
git clone --recursive https://github.com/ClickHouse/ClickHouse.git -b intel_accelerators

# Build:
For generic instructions, please refer to: https://clickhouse.com/docs/en/development/build
Recommend to use clang14 since our CK baseline is 21.12, otherwise some unexpected build errors may happen.

Build options for enable QAT:
- out-of-tree build: -DENABLE_QAT_OUTOFTREE=ON -DICP_ROOT=xxx
- in-tree build: -DENABLE_QAT_OUTOFTREE=OFF
- To enable USDM: -DENABLE_USDM_DRV=ON

For example, to build clickhouse with out-of-tree QAT support and USDM enabled:
$ mkdir build && cd build
$ cmake -DCMAKE_BUILD_TYPE=Release -DENABLE_QAT_OUTOFTREE=ON -DICP_ROOT=xxx -DENABLE_USDM_DRV=ON ..

Notice: 
-DICP_ROOT=xxx means to define the path of out-of-tree driver package; 
If you want out-of-tree build but have no package available, please download and build ICP package from: https://www.intel.com/content/www/us/en/download/765501.html

Reference:
out-of-tree/in-tree: https://github.com/intel/QAT-ZSTD-Plugin?tab=readme-ov-file#installation-instructions
USDM: https://github.com/intel/QAT-ZSTD-Plugin?tab=readme-ov-file#build-qat-sequence-producer-library-with-usdm-support

# Benchmark
Run clickhouse with qatzstd, set codec in config.xml as below:
    <compression>
        <case>
            <method>ZSTD_QAT</method>
            <level>1</level>
        </case>
    </compression>

To measure latency performance, please refer to the section of "Load the data" in below script:
https://github.com/ClickHouse/ClickBench/blob/main/clickhouse-cloud/benchmark.sh

# QAT Setup:
For generic setup, please refer to: https://github.com/intel/QAT-ZSTD-Plugin?tab=readme-ov-file#build-and-install-intel-quickassist-technology-driver

Set the environment variable "QAT_SECTION_NAME" aligned with your setup, please look into /etc/4xxx_dev0.conf with the section of "User Process Instance Section"
For example:
##############################################
# User Process Instance Section
##############################################
[SSL]
NumberCyInstances = 0
NumberDcInstances = 2
NumProcesses = 1
LimitDevAccess = 0
......

Then you should set it like below:
$ export QAT_SECTION_NAME=SSL
-------------------
