#!/bin/bash
# Verify that an image without a shell (the default `scratch` one, or a distroless one) contains no shell.
# This is the key property of these images: /bin/sh, /bin/bash,
# and other shells must be absent to reduce the attack surface.
set -eo pipefail

image="$1"

if docker run --rm --entrypoint /bin/sh "$image" -c "echo bad" 2>/dev/null; then
    echo "FAIL: /bin/sh should not exist in the image" >&2
    exit 1
fi

if docker run --rm --entrypoint /bin/bash "$image" -c "echo bad" 2>/dev/null; then
    echo "FAIL: /bin/bash should not exist in the image" >&2
    exit 1
fi

if docker run --rm --entrypoint /busybox/sh "$image" -c "echo bad" 2>/dev/null; then
    echo "FAIL: /busybox/sh should not exist in the image" >&2
    exit 1
fi
