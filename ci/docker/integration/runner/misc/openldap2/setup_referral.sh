#!/bin/bash
set -euo pipefail

SLAPD_D="/opt/bitnami/openldap/etc/slapd.d"
SUFFIX="dc=referral,dc=org"
REF_URL="ldap://openldap:1389/"

echo "Searching cn=config for database serving suffix: ${SUFFIX}"

# Find the slapd.d entry file that corresponds to the MDB database for this suffix
TARGET_LDIF="$(grep -Rsl "^olcSuffix: ${SUFFIX}$" "${SLAPD_D}/cn=config" | head -n 1 || true)"
if [[ -z "${TARGET_LDIF}" ]]; then
  echo "ERROR: Could not find cn=config entry with olcSuffix: ${SUFFIX}"
  echo "DEBUG: available suffixes:"
  grep -Rhs "^olcSuffix:" "${SLAPD_D}/cn=config" || true
  exit 1
fi

echo "Found target config file: ${TARGET_LDIF}"

# If referral already set, keep idempotent behavior
if grep -q "^olcReferral: " "${TARGET_LDIF}"; then
  echo "olcReferral already present, not modifying."
else
  echo "Adding olcReferral: ${REF_URL}"
  tmp="$(mktemp)"
  awk -v suffix="olcSuffix: ${SUFFIX}" -v ref="olcReferral: ${REF_URL}" '
    { print }
    $0 == suffix { print ref }
  ' "${TARGET_LDIF}" > "${tmp}"
  cat "${tmp}" > "${TARGET_LDIF}"
  rm -f "${tmp}"
fi

echo "Verifying olcReferral was applied..."
grep -q "^olcReferral: ${REF_URL}$" "${TARGET_LDIF}"

echo "Validating slapd config..."
/opt/bitnami/openldap/sbin/slaptest -F "${SLAPD_D}" -u

touch /tmp/.openldap-initialized
echo "openldap2 referral bootstrap complete."
