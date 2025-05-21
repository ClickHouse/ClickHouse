#!/usr/bin/env bash

export XRAY_OPTIONS="xray_mode=xray-basic verbosity=1"

res=$(
rm xray-log.* ; \
  ninja xray_tracing -C build > /dev/null 2>&1 && \
  build/src/Common/examples/xray_tracing 2 > /dev/null && \
  llvm-xray convert --symbolize --instr_map=build/src/Common/examples/xray_tracing --output-format=trace_event "$(ls -lt xray-log.* | awk '{ print $NF }' | tail -n1)" \
  | jq '.traceEvents | length'
)

echo "res: $res"

if [ "$res" -eq "12" ]; then
  echo -e "Test \033[32mpassed\033[0m"
else
  echo -e "Test \033[31mfailed\033[0m"
  exit 1
fi
