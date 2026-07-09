---
description: 'Documentation de la politique de masquage'
sidebar_label: 'POLITIQUE DE MASQUAGE'
sidebar_position: 42
slug: /sql-reference/statements/create/masking-policy
title: 'CREATE MASKING POLICY'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

Crée une politique de masquage, qui permet de transformer ou de masquer dynamiquement les valeurs des colonnes pour des utilisateurs ou rôles spécifiques lorsqu’ils interrogent une table.

:::tip
Les politiques de masquage assurent la sécurité des données au niveau des colonnes en transformant les données sensibles au moment de l’exécution de la requête, sans modifier les données stockées.
:::

Syntaxe :

```sql
CREATE MASKING POLICY [IF NOT EXISTS | OR REPLACE] policy_name ON [database.]table
    UPDATE column1 = expression1 [, column2 = expression2 ...]
    [WHERE condition]
    TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}
    [PRIORITY priority_number]
```

<div id="update-clause">
  ## Clause UPDATE
</div>

La clause `UPDATE` spécifie quelles colonnes masquer et comment les transformer. Vous pouvez masquer plusieurs colonnes dans une même politique.

Exemples :

* Masquage simple : `UPDATE email = '***masked***'`
* Masquage partiel : `UPDATE email = concat(substring(email, 1, 3), '***@***.***')`
* Masquage basé sur un hachage : `UPDATE email = concat('masked_', substring(hex(cityHash64(email)), 1, 8))`
* Plusieurs colonnes : `UPDATE email = '***@***.***', phone = '***-***-****'`

<div id="where-clause">
  ## Clause `WHERE`
</div>

La clause `WHERE`, facultative, permet un masquage conditionnel en fonction des valeurs des lignes. Seules les lignes qui remplissent la condition seront masquées.

Exemple :

```sql
CREATE MASKING POLICY mask_high_salaries ON employees
UPDATE salary = 0
WHERE salary > 100000
TO analyst;
```

<div id="to-clause">
  ## Clause TO
</div>

Dans la section `TO`, indiquez à quels utilisateurs et rôles la politique doit s’appliquer.

* `TO user1, user2` : s’applique à des utilisateurs/rôles spécifiques
* `TO ALL` : s’applique à tous les utilisateurs
* `TO ALL EXCEPT user1, user2` : s’applique à tous les utilisateurs, sauf ceux spécifiés

:::note
Contrairement aux row policies, les politiques de masquage n’affectent pas les utilisateurs auxquels elles ne s’appliquent pas. Si aucune politique de masquage ne s’applique à un utilisateur, celui-ci voit les données d’origine.
:::

<div id="priority-clause">
  ## Clause PRIORITY
</div>

Lorsque plusieurs politiques de masquage ciblent la même colonne pour un utilisateur, la clause `PRIORITY` détermine l’ordre d’application. Les politiques sont appliquées de la priorité la plus élevée à la plus faible.

La priorité par défaut est de 0. Les politiques ayant la même priorité sont appliquées dans un ordre indéfini.

Exemple :

```sql
-- Applied second (lower priority)
CREATE MASKING POLICY mask1 ON users
UPDATE email = 'low@priority.com'
TO analyst
PRIORITY 1;

-- Applied first (higher priority)
CREATE MASKING POLICY mask2 ON users
UPDATE email = 'high@priority.com'
TO analyst
PRIORITY 10;

-- analyst sees 'low@priority.com' because it's applied last
```

:::note Considérations relatives aux performances

* Les politiques de masquage peuvent affecter les performances des requêtes selon la complexité de l&#39;expression
* Certaines optimisations peuvent être désactivées sur les tables auxquelles des politiques de masquage actives sont appliquées
  :::