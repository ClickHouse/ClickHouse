---
description: 'Overview of external authentication methods supported by ClickHouse'
pagination_next: operations/external-authenticators/kerberos
sidebar_label: 'External User Authenticators and Directories'
sidebar_position: 48
slug: /operations/external-authenticators/
title: 'External User Authenticators and Directories'
doc_type: 'reference'
---

ClickHouse supports authenticating and managing users using external services.

The following external authenticators and directories are supported:

- [LDAP](/operations/external-authenticators/ldap#ldap-external-authenticator) [Authenticator](./ldap.md#ldap-external-authenticator) and [Directory](./ldap.md#ldap-external-user-directory)
- Kerberos [Authenticator](/operations/external-authenticators/kerberos#kerberos-as-an-external-authenticator-for-existing-users)
- [SSL X.509 authentication](/operations/external-authenticators/ssl-x509)
- HTTP [Authenticator](./http.md)
<<<<<<< HEAD
- [JWT authentication](/operations/external-authenticators/jwt)
=======
- Token-based [Authenticator](./tokens.md)
>>>>>>> ad5b91c853d (Merge pull request #1658 from Altinity/feature/antalya-26.3/pr-1430-1596)
