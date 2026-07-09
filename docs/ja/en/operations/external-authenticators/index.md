---
description: 'ClickHouse でサポートされる外部認証方式の概要'
pagination_next: operations/external-authenticators/kerberos
sidebar_label: '外部ユーザー認証およびディレクトリ'
sidebar_position: 48
slug: /operations/external-authenticators/
title: '外部ユーザー認証およびディレクトリ'
doc_type: 'reference'
---

ClickHouse は、外部サービスを使用したユーザーの認証と管理をサポートしています。

サポートされている外部認証およびディレクトリは次のとおりです。

* [LDAP](/ja/operations/external-authenticators/ldap#ldap-external-authenticator) [認証](./ldap.md#ldap-external-authenticator) と [ディレクトリ](./ldap.md#ldap-external-user-directory)
* Kerberos [認証](/ja/operations/external-authenticators/kerberos#kerberos-as-an-external-authenticator-for-existing-users)
* [SSL X.509 認証](/ja/operations/external-authenticators/ssl-x509)
* HTTP [認証](./http.md)
* [JWT 認証](/ja/operations/external-authenticators/jwt)