---
slug: /en/operations/external-authenticators/
sidebar_position: 48
sidebar_label: External User Authenticators and Directories
title: "External User Authenticators and Directories"
pagination_next: 'en/operations/external-authenticators/kerberos'
---
import SelfManaged from '@site/docs/en/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

ClickHouse supports authenticating and managing users using external services.

The following external authenticators and directories are supported:

- [LDAP](./ldap.md#external-authenticators-ldap) [Authenticator](./ldap.md#ldap-external-authenticator) and [Directory](./ldap.md#ldap-external-user-directory)
- Kerberos [Authenticator](./kerberos.md#external-authenticators-kerberos)
- [SSL X.509 authentication](./ssl-x509.md#ssl-external-authentication)
- HTTP [Authenticator](./http.md)