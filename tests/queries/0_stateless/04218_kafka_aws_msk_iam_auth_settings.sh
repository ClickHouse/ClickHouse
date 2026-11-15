#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest -- requires Kafka

# Stateless tests for AWS MSK IAM auth setting validation.
# These run against CLICKHOUSE_LOCAL (no real Kafka needed): the BAD_ARGUMENTS
# exceptions fire inside popConsumer() before any network I/O.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -o pipefail

# ---------------------------------------------------------------------------
# 1. Non-MSK broker with no explicit region must fail with BAD_ARGUMENTS (36).
# ---------------------------------------------------------------------------
$CLICKHOUSE_LOCAL -nm -q "
CREATE TABLE t (v String)
ENGINE = Kafka
SETTINGS
    kafka_broker_list    = 'localhost:9092',
    kafka_topic_list     = 'test',
    kafka_group_name     = 'g1',
    kafka_format         = 'JSONEachRow',
    kafka_sasl_mechanism = 'AWS_MSK_IAM';

SELECT * FROM t LIMIT 1 SETTINGS stream_like_engine_allow_direct_select = 1;
" 2>&1 | grep -o "Code: 36" | head -1

# ---------------------------------------------------------------------------
# 2. Explicit kafka_aws_region with invalid format must fail with BAD_ARGUMENTS.
# ---------------------------------------------------------------------------
$CLICKHOUSE_LOCAL -nm -q "
CREATE TABLE t (v String)
ENGINE = Kafka
SETTINGS
    kafka_broker_list      = 'localhost:9092',
    kafka_topic_list       = 'test',
    kafka_group_name       = 'g1',
    kafka_format           = 'JSONEachRow',
    kafka_sasl_mechanism   = 'AWS_MSK_IAM',
    kafka_aws_region       = 'INVALID_REGION';

SELECT * FROM t LIMIT 1 SETTINGS stream_like_engine_allow_direct_select = 1;
" 2>&1 | grep -o "Code: 36" | head -1

# ---------------------------------------------------------------------------
# 3. Valid GovCloud region must not fail with BAD_ARGUMENTS.
#    The SELECT will fail later (connection refused / no AWS creds), but the
#    error code must NOT be 36.
# ---------------------------------------------------------------------------
$CLICKHOUSE_LOCAL -nm -q "
CREATE TABLE t (v String)
ENGINE = Kafka
SETTINGS
    kafka_broker_list      = 'b-1.cluster.kafka.us-gov-west-1.amazonaws.com:9098',
    kafka_topic_list       = 'test',
    kafka_group_name       = 'g1',
    kafka_format           = 'JSONEachRow',
    kafka_sasl_mechanism   = 'AWS_MSK_IAM',
    kafka_aws_region       = 'us-gov-west-1';

SELECT * FROM t LIMIT 1 SETTINGS stream_like_engine_allow_direct_select = 1;
" 2>&1 | grep -v "^$" | grep -o "Code: 36" | head -1 || echo "OK"

# ---------------------------------------------------------------------------
# 4. MSK broker address allows region auto-detection (no explicit region needed).
#    Same expectation as test 3: must not produce Code: 36.
# ---------------------------------------------------------------------------
$CLICKHOUSE_LOCAL -nm -q "
CREATE TABLE t (v String)
ENGINE = Kafka
SETTINGS
    kafka_broker_list      = 'b-1.cluster.kafka.us-east-1.amazonaws.com:9098',
    kafka_topic_list       = 'test',
    kafka_group_name       = 'g1',
    kafka_format           = 'JSONEachRow',
    kafka_sasl_mechanism   = 'AWS_MSK_IAM';

SELECT * FROM t LIMIT 1 SETTINGS stream_like_engine_allow_direct_select = 1;
" 2>&1 | grep -o "Code: 36" | head -1 || echo "OK"

# ---------------------------------------------------------------------------
# 5. Non-MSK broker + explicit valid region: region cannot be inferred but is
#    provided manually, so BAD_ARGUMENTS must NOT be raised.
# ---------------------------------------------------------------------------
$CLICKHOUSE_LOCAL -nm -q "
CREATE TABLE t (v String)
ENGINE = Kafka
SETTINGS
    kafka_broker_list      = 'localhost:9092',
    kafka_topic_list       = 'test',
    kafka_group_name       = 'g1',
    kafka_format           = 'JSONEachRow',
    kafka_sasl_mechanism   = 'AWS_MSK_IAM',
    kafka_aws_region       = 'us-east-1';

SELECT * FROM t LIMIT 1 SETTINGS stream_like_engine_allow_direct_select = 1;
" 2>&1 | grep -o "Code: 36" | head -1 || echo "OK"
