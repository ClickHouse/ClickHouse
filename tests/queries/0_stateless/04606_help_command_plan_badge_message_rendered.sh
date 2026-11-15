#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the regression target is the `S3Queue` table engine, which needs the AWS S3 support
# that is not built into the Fast test.

# A plan-gating badge such as `<ScalePlanFeatureBadge feature="S3 Role-Based Access" />` renders a
# substantive message from its attributes on the website (which plan the feature requires and how to
# get it, see `docs/snippets/components/ScalePlanFeatureBadge`). The terminal `help` renderer must
# render that message after the badge label (see `badgePayload` in `TerminalMarkdownRenderer.cpp`)
# instead of collapsing the whole tag to the badge name and losing the plan-gating warning.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The output of `clickhouse-local -q` is a (non-tty) pipe, so the rendering is deterministic plain text.
out=$($CLICKHOUSE_LOCAL -q "help S3Queue")

# Flatten word-wrap before matching: the message may be wrapped across lines.
flat=$(printf '%s' "$out" | tr '\n' ' ' | tr -s ' ')

printf '%s' "$flat" | grep -qF "[Scale plan feature]" \
    && echo "OK: badge label is rendered" \
    || echo "FAIL: missing badge label"
printf '%s' "$flat" | grep -qF "S3 Role-Based Access is available in the Scale and Enterprise plans. To upgrade, visit the plans page in the cloud console." \
    && echo "OK: badge message is rendered" \
    || echo "FAIL: missing badge message"
printf '%s' "$out" | grep -qF "<ScalePlanFeatureBadge" \
    && echo "FAIL: raw badge tag still present" \
    || echo "OK: raw badge tag is gone"
