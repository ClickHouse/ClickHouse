#!/bin/bash
# Bootstrap for the `openldap_strict` integration-test fixture
# (tests/integration/compose/docker_compose_ldap_strict.yml).
#
# The bitnami image runs the scripts in /docker-entrypoint-initdb.d with slapd
# STOPPED (see `ldap_custom_init_scripts` in /opt/bitnami/scripts/libopenldap.sh:
# `ldap_initialize` ends with `ldap_stop` and the scripts run afterwards), so
# everything below is done offline with the slap* tools against slapd.d and the
# mdb database, and validated with `slaptest -u` before slapd starts.
#
# The fixture models a directory where ordinary users cannot search for groups
# or read their own `memberOf`, which is what forces ClickHouse to run role
# searches under a dedicated service account (`lookup_bind_dn`):
#   - anonymous binds are refused (`LDAP_ALLOW_ANON_BINDING=no` in the compose
#     file sets `olcDisallows: bind_anon` + `olcRequires: authc`; re-asserted here);
#   - the `memberof` overlay maintains `memberOf` on user entries for groups
#     added through LDAP operations at runtime (never for `slapadd`-ed entries);
#   - ACLs: users may read the tree except `ou=groups` and `memberOf`, which only
#     the service account `cn=svc.clickhouse,ou=service,dc=example,dc=org` may read;
#   - the service account has no size limits (needed for paged enumeration);
#   - extra users whose names need RFC 4514/4515 escaping, and a duplicated `uid`
#     across two containers to test the "more than one entry" path.
set -euo pipefail

SLAPD_D="/opt/bitnami/openldap/etc/slapd.d"
SBIN="/opt/bitnami/openldap/sbin"
# `olcModulePath` of the bitnami image points to libexec/openldap, which does not
# contain the overlays, so the module is loaded by absolute path.
MEMBEROF_MODULE="/opt/bitnami/openldap/lib/openldap/memberof.so"
SUFFIX="dc=example,dc=org"
SVC_DN="cn=svc.clickhouse,ou=service,${SUFFIX}"
SVC_PASSWORD="svcsecret"
USER_PASSWORD="qwerty"
WORK_DIR="$(mktemp -d)"

echo "openldap_strict: locating the mdb database serving ${SUFFIX}"
DB_LDIF="$(grep -Rsl "^olcSuffix: ${SUFFIX}$" "${SLAPD_D}/cn=config" | head -n 1 || true)"
if [[ -z "${DB_LDIF}" ]]; then
    echo "ERROR: no cn=config entry with olcSuffix: ${SUFFIX}"
    grep -Rhs "^olcSuffix:" "${SLAPD_D}/cn=config" || true
    exit 1
fi
# e.g. olcDatabase={2}mdb
DB_RDN="$(basename "${DB_LDIF}" .ldif)"
echo "openldap_strict: database entry is ${DB_RDN}"

MODULE_LDIF="$(grep -Rsl "^olcModuleLoad:" "${SLAPD_D}/cn=config" | head -n 1 || true)"
if [[ -z "${MODULE_LDIF}" ]]; then
    echo "ERROR: no olcModuleList entry found under ${SLAPD_D}/cn=config"
    exit 1
fi
MODULE_RDN="$(basename "${MODULE_LDIF}" .ldif)"
echo "openldap_strict: module list entry is ${MODULE_RDN}"

echo "openldap_strict: step 1/5 - global settings and memberof module"
{
    if ! grep -q "^olcDisallows: bind_anon$" "${SLAPD_D}/cn=config.ldif"; then
        cat <<LDIF
dn: cn=config
changetype: modify
add: olcDisallows
olcDisallows: bind_anon

LDIF
    fi
    if ! grep -q "^olcModuleLoad: .*memberof" "${MODULE_LDIF}"; then
        cat <<LDIF
dn: ${MODULE_RDN},cn=config
changetype: modify
add: olcModuleLoad
olcModuleLoad: ${MEMBEROF_MODULE}

LDIF
    fi
} > "${WORK_DIR}/global.ldif"
if [[ -s "${WORK_DIR}/global.ldif" ]]; then
    "${SBIN}/slapmodify" -F "${SLAPD_D}" -n 0 -l "${WORK_DIR}/global.ldif"
fi

echo "openldap_strict: step 2/5 - ACLs and limits on ${DB_RDN}"
if ! grep -q "^olcAccess:" "${DB_LDIF}"; then
    cat > "${WORK_DIR}/acl.ldif" <<LDIF
dn: ${DB_RDN},cn=config
changetype: modify
add: olcAccess
olcAccess: {0}to attrs=userPassword by self write by anonymous auth by * none
olcAccess: {1}to dn.subtree="ou=groups,${SUFFIX}" by dn.exact="${SVC_DN}" read by * none
olcAccess: {2}to attrs=memberOf by dn.exact="${SVC_DN}" read by * none
olcAccess: {3}to * by dn.exact="${SVC_DN}" read by users read by * none
-
add: olcLimits
olcLimits: {0}dn.exact="${SVC_DN}" size=unlimited size.prtotal=unlimited
LDIF
    "${SBIN}/slapmodify" -F "${SLAPD_D}" -n 0 -l "${WORK_DIR}/acl.ldif"
else
    echo "olcAccess already present on ${DB_RDN}, not modifying."
fi

echo "openldap_strict: step 3/5 - memberof overlay on ${DB_RDN}"
if [[ ! -d "${SLAPD_D}/cn=config/${DB_RDN}" ]] || ! ls "${SLAPD_D}/cn=config/${DB_RDN}" | grep -q memberof; then
    cat > "${WORK_DIR}/overlay.ldif" <<LDIF
dn: olcOverlay={0}memberof,${DB_RDN},cn=config
objectClass: olcOverlayConfig
objectClass: olcMemberOf
olcOverlay: {0}memberof
olcMemberOfRefInt: TRUE
olcMemberOfGroupOC: groupOfNames
olcMemberOfMemberAD: member
olcMemberOfMemberOfAD: memberOf
LDIF
    "${SBIN}/slapadd" -F "${SLAPD_D}" -n 0 -l "${WORK_DIR}/overlay.ldif"
else
    echo "memberof overlay already present on ${DB_RDN}, not modifying."
fi

echo "openldap_strict: step 4/5 - service account, group container and special users"
cat > "${WORK_DIR}/data.ldif" <<LDIF
dn: ou=groups,${SUFFIX}
objectClass: organizationalUnit
ou: groups

dn: ou=service,${SUFFIX}
objectClass: organizationalUnit
ou: service

dn: ${SVC_DN}
objectClass: organizationalRole
objectClass: simpleSecurityObject
cn: svc.clickhouse
description: Service account used by ClickHouse for user DN detection and role searches
userPassword: ${SVC_PASSWORD}

dn: cn=special(user)*,ou=users,${SUFFIX}
objectClass: inetOrgPerson
cn: special(user)*
sn: Special
uid: special(user)*
userPassword: ${USER_PASSWORD}

dn: cn=oneil,ou=users,${SUFFIX}
objectClass: inetOrgPerson
cn: oneil
sn: O'Neil
uid: o'neil,doe
userPassword: ${USER_PASSWORD}

dn: cn=aeqb,ou=users,${SUFFIX}
objectClass: inetOrgPerson
cn: aeqb
sn: Equals
uid: a=b
userPassword: ${USER_PASSWORD}

dn: cn=dupuser,ou=users,${SUFFIX}
objectClass: inetOrgPerson
cn: dupuser
sn: Duplicate
uid: dupuser
userPassword: ${USER_PASSWORD}

dn: cn=dupuser,ou=service,${SUFFIX}
objectClass: inetOrgPerson
cn: dupuser
sn: Duplicate
uid: dupuser
userPassword: ${USER_PASSWORD}
LDIF
"${SBIN}/slapadd" -F "${SLAPD_D}" -b "${SUFFIX}" -l "${WORK_DIR}/data.ldif"

echo "openldap_strict: step 5/5 - validating slapd configuration"
"${SBIN}/slaptest" -F "${SLAPD_D}" -u

rm -rf "${WORK_DIR}"
touch /tmp/.openldap-initialized
echo "openldap_strict: bootstrap complete."
