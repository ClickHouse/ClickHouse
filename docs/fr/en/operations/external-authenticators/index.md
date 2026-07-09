---
description: 'Présentation des méthodes d’authentification externes prises en charge par ClickHouse'
pagination_next: operations/external-authenticators/kerberos
sidebar_label: 'Authentificateurs externes et annuaires d’utilisateurs'
sidebar_position: 48
slug: /operations/external-authenticators/
title: 'Authentificateurs externes et annuaires d’utilisateurs'
doc_type: 'reference'
---

ClickHouse prend en charge l’authentification et la gestion des utilisateurs via des services externes.

Les authentificateurs externes et les annuaires d’utilisateurs suivants sont pris en charge :

* [LDAP](/fr/operations/external-authenticators/ldap#ldap-external-authenticator) [Authentification](./ldap.md#ldap-external-authenticator) et [Annuaire](./ldap.md#ldap-external-user-directory)
* Kerberos [Authentification](/fr/operations/external-authenticators/kerberos#kerberos-as-an-external-authenticator-for-existing-users)
* [Authentification SSL X.509](/fr/operations/external-authenticators/ssl-x509)
* HTTP [Authentification](./http.md)
* [Authentification JWT](/fr/operations/external-authenticators/jwt)