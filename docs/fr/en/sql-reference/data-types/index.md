---
description: 'Documentation sur les types de données dans ClickHouse'
sidebar_label: 'Liste des types de données'
sidebar_position: 1
slug: /sql-reference/data-types/
title: 'Types de données dans ClickHouse'
doc_type: 'reference'
---

Cette section décrit les types de données pris en charge par ClickHouse, par exemple les [entiers](int-uint.md), les [flottants](float.md) et les [chaînes](string.md).

La table système [system.data&#95;type&#95;families](/fr/operations/system-tables/data_type_families) fournit une
vue d’ensemble de tous les types de données disponibles.
Elle indique également si un type de données est un alias d’un autre type de données et si son nom distingue les majuscules des minuscules (par ex. `bool` vs. `BOOL`).