---
slug: /ja/operations/external-authenticators/
sidebar_position: 48
sidebar_label: 外部ユーザー認証者とディレクトリ
title: "外部ユーザー認証者とディレクトリ"
pagination_next: 'en/operations/external-authenticators/kerberos'
---
import SelfManaged from '@site/docs/ja/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

ClickHouseは、外部サービスを使用してユーザーの認証と管理をサポートしています。

サポートされる外部認証者とディレクトリは以下の通りです：

- [LDAP](./ldap.md#external-authenticators-ldap) [認証者](./ldap.md#ldap-external-authenticator) および [ディレクトリ](./ldap.md#ldap-external-user-directory)
- Kerberos [認証者](./kerberos.md#external-authenticators-kerberos)
- [SSL X.509 認証](./ssl-x509.md#ssl-external-authentication)
- HTTP [認証者](./http.md)
