#!/bin/bash

curl -sS 'http://localhost:8123/?extremes=1' -d "SELECT 1 AS k, count() GROUP BY k WITH TOTALS";
curl -sS 'http://localhost:8123/?extremes=1' -d "SELECT 1234567890123 AS k, count() GROUP BY k WITH TOTALS FORMAT JSON";
curl -sS 'http://localhost:8123/?extremes=1' -d "SELECT toFloat32(1.23) AS k, count() GROUP BY k WITH TOTALS FORMAT JSONCompact";

curl -sS 'http://localhost:8123/?extremes=1' -d "SELECT toDate('2010-01-01') AS k, count() GROUP BY k WITH TOTALS";
curl -sS 'http://localhost:8123/?extremes=1' -d "SELECT toDateTime('2010-01-01 01:02:03') AS k, count() GROUP BY k WITH TOTALS FORMAT JSON";
curl -sS 'http://localhost:8123/?extremes=1' -d "SELECT 1.1 AS k, count() GROUP BY k WITH TOTALS FORMAT JSONCompact";
