---
description: 'نظرة عامة على أساليب المصادقة الخارجية التي يدعمها ClickHouse'
pagination_next: operations/external-authenticators/kerberos
sidebar_label: 'وسائل مصادقة المستخدمين الخارجية والدلائل'
sidebar_position: 48
slug: /operations/external-authenticators/
title: 'وسائل مصادقة المستخدمين الخارجية والدلائل'
doc_type: 'مرجع'
---

يدعم ClickHouse مصادقة المستخدمين وإدارتهم باستخدام خدمات خارجية.

وسائل المصادقة الخارجية والدلائل التالية مدعومة:

* [LDAP](/ar/operations/external-authenticators/ldap#ldap-external-authenticator) [للمصادقة](./ldap.md#ldap-external-authenticator) و[الدليل](./ldap.md#ldap-external-user-directory)
* Kerberos [للمصادقة](/ar/operations/external-authenticators/kerberos#kerberos-as-an-external-authenticator-for-existing-users)
* [مصادقة SSL X.509](/ar/operations/external-authenticators/ssl-x509)
* HTTP [للمصادقة](./http.md)
* [مصادقة JWT](/ar/operations/external-authenticators/jwt)