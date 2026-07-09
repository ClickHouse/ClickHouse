---
description: 'ClickHouse 支持的外部身份验证方法概述'
pagination_next: operations/external-authenticators/kerberos
sidebar_label: '外部用户身份验证器和目录'
sidebar_position: 48
slug: /operations/external-authenticators/
title: '外部用户身份验证器和目录'
doc_type: 'reference'
---

ClickHouse 支持通过外部服务对用户进行身份验证和管理。

支持以下外部身份验证器和目录：

* [LDAP](/zh/operations/external-authenticators/ldap#ldap-external-authenticator) [身份验证器](./ldap.md#ldap-external-authenticator) 和 [目录](./ldap.md#ldap-external-user-directory)
* Kerberos [身份验证器](/zh/operations/external-authenticators/kerberos#kerberos-as-an-external-authenticator-for-existing-users)
* [SSL X.509 身份验证](/zh/operations/external-authenticators/ssl-x509)
* HTTP [身份验证器](./http.md)
* [JWT 身份验证](/zh/operations/external-authenticators/jwt)