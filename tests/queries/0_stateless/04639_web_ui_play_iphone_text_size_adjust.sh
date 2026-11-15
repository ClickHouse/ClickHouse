#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for broken SQL syntax highlighting in the Web UI on iPhone.
# Mobile Safari's automatic text inflation rescaled the `#query` textarea and the
# `#query-backdrop` div behind it by different multipliers, misaligning the
# highlighting overlay. The fix disables the inflation with `text-size-adjust: 100%`
# and adds a `device-width` viewport, compensating the focus-zoom side effect by
# raising form-control fonts to 16px on touch devices.

PLAY_PAGE=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/play")

# Both the prefixed and the standard property are needed: only the prefixed one is honored by Safari.
UNINDENTED_PAGE=$(echo "$PLAY_PAGE" | sed -e 's/^[[:space:]]*//')
echo "$UNINDENTED_PAGE" | grep -x -F -m1 -- '-webkit-text-size-adjust: 100%;'
echo "$UNINDENTED_PAGE" | grep -x -F -m1 'text-size-adjust: 100%;'
echo "$PLAY_PAGE" | grep -o -F -m1 '<meta name="viewport" content="width=device-width, initial-scale=1">'

# The `device-width` viewport makes mobile Safari zoom the page when a control with a font
# smaller than 16px is focused, so the coarse-pointer rule must keep raising every layer that
# mirrors the textarea's metrics to exactly 16px. Check the declaration and each selector, not
# just the media query header: dropping one of the layers would silently misalign the overlay again.
COARSE_BLOCK=$(echo "$PLAY_PAGE" | grep -A 5 -F -- '@media (hover: none) and (pointer: coarse)' | tr '\n' ' ' | tr -s ' ')

echo "$COARSE_BLOCK" | grep -o -F -m1 -- '@media (hover: none) and (pointer: coarse)'
echo "$COARSE_BLOCK" | grep -o -F -m1 'font-size: 16px;'

SELECTOR_LIST=$(echo "$COARSE_BLOCK" | sed -e 's/.*coarse) {//' -e 's/{.*//' | tr ',' '\n' | tr -d ' ')

for SELECTOR in 'input' 'textarea' '#query-backdrop' '#completion-mirror' '.completion-entry' '#url_status'
do
    if echo "$SELECTOR_LIST" | grep -q -x -F -- "$SELECTOR"
    then
        echo "$SELECTOR is raised to 16px"
    else
        echo "$SELECTOR is missing from the coarse-pointer rule"
    fi
done
