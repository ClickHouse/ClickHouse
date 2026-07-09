---
description: 'Descripción general de los métodos de autenticación externos compatibles con ClickHouse'
pagination_next: operations/external-authenticators/kerberos
sidebar_label: 'Autenticadores externos y directorios de usuarios'
sidebar_position: 48
slug: /operations/external-authenticators/
title: 'Autenticadores externos y directorios de usuarios'
doc_type: 'reference'
---

ClickHouse permite autenticar y gestionar usuarios mediante servicios externos.

Se admiten los siguientes autenticadores y directorios externos:

* [LDAP](/es/operations/external-authenticators/ldap#ldap-external-authenticator) [Autenticador](./ldap.md#ldap-external-authenticator) y [Directorio](./ldap.md#ldap-external-user-directory)
* Kerberos [Autenticador](/es/operations/external-authenticators/kerberos#kerberos-as-an-external-authenticator-for-existing-users)
* [Autenticación SSL X.509](/es/operations/external-authenticators/ssl-x509)
* HTTP [Autenticador](./http.md)
* [Autenticación JWT](/es/operations/external-authenticators/jwt)