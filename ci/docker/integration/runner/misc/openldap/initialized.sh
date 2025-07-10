#!/bin/bash
set -e

# workaround for https://github.com/bitnami/containers/issues/73310
touch /tmp/.openldap-initialized
