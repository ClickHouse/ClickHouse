---
slug: /operations/external-authenticators/
sidebar_position: 48
sidebar_label: External User Authenticators and Directories
title: "External User Authenticators and Directories"
pagination_next: 'operations/external-authenticators/kerberos'
---
import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

ClickHouse supports authenticating and managing users using external services.

The following external authenticators and directories are supported:

- [LDAP](/operations/external-authenticators/ldap#ldap-external-authenticator) [Authenticator](./ldap.md#ldap-external-authenticator) and [Directory](./ldap.md#ldap-external-user-directory)
- Kerberos [Authenticator](/operations/external-authenticators/kerberos#kerberos-as-an-external-authenticator-for-existing-users)
- [SSL X.509 authentication](/operations/external-authenticators/ssl-x509)
- HTTP [Authenticator](./http.md)