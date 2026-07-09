---
description: 'ClickHouse에서 지원하는 외부 인증 메서드 개요'
pagination_next: operations/external-authenticators/kerberos
sidebar_label: '외부 사용자 인증자 및 디렉터리'
sidebar_position: 48
slug: /operations/external-authenticators/
title: '외부 사용자 인증자 및 디렉터리'
doc_type: 'reference'
---

ClickHouse는 외부 서비스를 사용한 사용자 인증 및 관리를 지원합니다.

다음과 같은 외부 인증자 및 디렉터리를 지원합니다.

* [LDAP](/ko/operations/external-authenticators/ldap#ldap-external-authenticator) [인증자](./ldap.md#ldap-external-authenticator) 및 [디렉터리](./ldap.md#ldap-external-user-directory)
* Kerberos [인증자](/ko/operations/external-authenticators/kerberos#kerberos-as-an-external-authenticator-for-existing-users)
* [SSL X.509 인증](/ko/operations/external-authenticators/ssl-x509)
* HTTP [인증자](./http.md)
* [JWT 인증](/ko/operations/external-authenticators/jwt)