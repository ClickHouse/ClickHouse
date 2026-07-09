---
description: 'Обзор внешних методов аутентификации, поддерживаемых ClickHouse'
pagination_next: operations/external-authenticators/kerberos
sidebar_label: 'Внешние аутентификаторы и каталоги пользователей'
sidebar_position: 48
slug: /operations/external-authenticators/
title: 'Внешние аутентификаторы и каталоги пользователей'
doc_type: 'reference'
---

ClickHouse поддерживает аутентификацию пользователей и управление ими с использованием внешних сервисов.

Поддерживаются следующие внешние аутентификаторы и каталоги пользователей:

* [LDAP](/ru/operations/external-authenticators/ldap#ldap-external-authenticator) [аутентификатор](./ldap.md#ldap-external-authenticator) и [каталог](./ldap.md#ldap-external-user-directory)
* Kerberos [аутентификатор](/ru/operations/external-authenticators/kerberos#kerberos-as-an-external-authenticator-for-existing-users)
* [Аутентификация SSL X.509](/ru/operations/external-authenticators/ssl-x509)
* HTTP [аутентификатор](./http.md)
* [Аутентификация JWT](/ru/operations/external-authenticators/jwt)