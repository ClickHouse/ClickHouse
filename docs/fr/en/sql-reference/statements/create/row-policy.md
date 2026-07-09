---
description: 'Documentation de référence pour ROW POLICY'
sidebar_label: 'ROW POLICY'
sidebar_position: 41
slug: /sql-reference/statements/create/row-policy
title: 'CREATE ROW POLICY'
doc_type: 'référence'
---

Crée une [politique de ligne](../../../guides/sre/user-management/index.md#row-policy-management), c’est-à-dire un filtre servant à déterminer quelles lignes d’une table un utilisateur peut lire.

:::tip
Les politiques de ligne n’ont de sens que pour les utilisateurs disposant d’un accès en lecture seule. Si un utilisateur peut modifier une table ou copier des partitions d’une table à l’autre, cela annule les restrictions imposées par les politiques de ligne.
:::

Syntaxe :

```sql
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] policy_name1 [ON CLUSTER cluster_name1] ON [db1.]table1|db1.*
        [, policy_name2 [ON CLUSTER cluster_name2] ON [db2.]table2|db2.* ...]
    [IN access_storage_type]
    [FOR SELECT] USING condition
    [AS {PERMISSIVE | RESTRICTIVE}]
    [TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}]
```

<div id="using-clause">
  ## Clause USING
</div>

Permet de spécifier une condition pour filtrer les lignes. Un utilisateur voit une ligne si la condition, évaluée pour cette ligne, renvoie une valeur non nulle.

<div id="to-clause">
  ## Clause TO
</div>

Dans la section `TO`, vous pouvez indiquer une liste d’utilisateurs et de rôles auxquels cette politique doit s’appliquer. Par exemple, `CREATE ROW POLICY ... TO accountant, john@localhost`.

Le mot-clé `ALL` désigne tous les utilisateurs ClickHouse, y compris l’utilisateur courant. Le mot-clé `ALL EXCEPT` permet d’exclure certains utilisateurs de la liste de tous les utilisateurs, par exemple `CREATE ROW POLICY ... TO ALL EXCEPT accountant, john@localhost`

<div id="as-clause">
  ## Clause AS
</div>

Il est possible d’avoir plusieurs politiques activées sur la même table pour un même utilisateur en même temps. Nous avons donc besoin d’un moyen de combiner les conditions de plusieurs politiques.

Par défaut, les politiques sont combinées à l’aide de l’opérateur booléen `OR`. Par exemple, les politiques suivantes :

```sql
CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 TO peter, antonio
```

permettre à l’utilisateur `peter` de voir les lignes où `b=1` ou `c=2`.

La clause `AS` précise comment les politiques doivent être combinées avec les autres politiques. Les politiques peuvent être permissives ou restrictives. Par défaut, les politiques sont permissives, ce qui signifie qu’elles sont combinées à l’aide de l’opérateur booléen `OR`.

Une politique peut aussi être définie comme restrictive. Les politiques restrictives sont combinées à l’aide de l’opérateur booléen `AND`.

Voici la formule générale :

```text
row_is_visible = (one or more of the permissive policies' conditions are non-zero) AND
                 (all of the restrictive policies's conditions are non-zero)
```

Par exemple, les politiques suivantes :

```sql
CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 AS RESTRICTIVE TO peter, antonio
```

permettent à l’utilisateur `peter` de voir des lignes uniquement si `b=1` et `c=2`.

Les politiques au niveau de la base de données sont combinées avec les politiques de table.

Par exemple, les politiques suivantes :

```sql
CREATE ROW POLICY pol1 ON mydb.* USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 AS RESTRICTIVE TO peter, antonio
```

permettre à l’utilisateur `peter` de ne voir les lignes de table1 que si `b=1` AND `c=2`, bien que
toute autre table de mydb n’ait que la politique `b=1` appliquée à l’utilisateur.

<div id="on-cluster-clause">
  ## Clause ON CLUSTER
</div>

Permet de créer des politiques de ligne sur un cluster, voir [DDL distribués](../../../sql-reference/distributed-ddl.md).

<div id="examples">
  ## Exemples
</div>

`CREATE ROW POLICY filter1 ON mydb.mytable USING a<1000 TO accountant, john@localhost`

`CREATE ROW POLICY filter2 ON mydb.mytable USING a<1000 AND b=5 TO ALL EXCEPT mira`

`CREATE ROW POLICY filter3 ON mydb.mytable USING 1 TO admin`

`CREATE ROW POLICY filter4 ON mydb.* USING 1 TO admin`