#!/usr/bin/env bash
echo "----CLICKHOUSE SHORTCUT CHECKS-----"
../../../build/programs/clickhouse --version
../../../build/programs/clickhouse --host sdfsadf --version
../../../build/programs/clickhouse -h sdfsadf --version
../../../build/programs/clickhouse --port 9000 --version
../../../build/programs/clickhouse --user qwr --version
../../../build/programs/clickhouse -u qwr --version
../../../build/programs/clickhouse --password secret --version

echo "----CH SHORTCUT CHECKS-----"
../../../build/programs/ch --version
../../../build/programs/ch --host sdfsadf --version
../../../build/programs/ch -h sdfsadf --version
../../../build/programs/ch --port 9000 --version
../../../build/programs/ch --user qwr --version
../../../build/programs/ch -u qwr --version
../../../build/programs/ch --password secret --version