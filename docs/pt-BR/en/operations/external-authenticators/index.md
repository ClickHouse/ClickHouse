---
description: 'Visão geral dos métodos de autenticação externos compatíveis com o ClickHouse'
pagination_next: operations/external-authenticators/kerberos
sidebar_label: 'Autenticadores externos e diretórios de usuários'
sidebar_position: 48
slug: /operations/external-authenticators/
title: 'Autenticadores externos e diretórios de usuários'
doc_type: 'reference'
---

O ClickHouse oferece suporte à autenticação e ao gerenciamento de usuários por meio de serviços externos.

Há suporte para os seguintes autenticadores e diretórios externos:

* [LDAP](/pt-BR/operations/external-authenticators/ldap#ldap-external-authenticator) [Autenticador](./ldap.md#ldap-external-authenticator) e [Diretório](./ldap.md#ldap-external-user-directory)
* Kerberos [Autenticador](/pt-BR/operations/external-authenticators/kerberos#kerberos-as-an-external-authenticator-for-existing-users)
* [autenticação SSL X.509](/pt-BR/operations/external-authenticators/ssl-x509)
* HTTP [Autenticador](./http.md)
* [autenticação JWT](/pt-BR/operations/external-authenticators/jwt)