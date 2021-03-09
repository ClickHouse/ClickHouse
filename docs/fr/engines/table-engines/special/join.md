---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 40
toc_title: Rejoindre
---

# Rejoindre {#join}

Structure de données préparée pour l'utilisation dans [JOIN](../../../sql-reference/statements/select/join.md#select-join) opérations.

## Création d'une Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
) ENGINE = Join(join_strictness, join_type, k1[, k2, ...])
```

Voir la description détaillée de la [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) requête.

**Les Paramètres Du Moteur**

-   `join_strictness` – [ADHÉRER à la rigueur](../../../sql-reference/statements/select/join.md#select-join-types).
-   `join_type` – [Type de jointure](../../../sql-reference/statements/select/join.md#select-join-types).
-   `k1[, k2, ...]` – Key columns from the `USING` la clause que l' `JOIN` l'opération est faite avec de la.

Entrer `join_strictness` et `join_type` paramètres sans guillemets, par exemple, `Join(ANY, LEFT, col1)`. Ils doivent correspondre à la `JOIN` fonctionnement que le tableau sera utilisé pour. Si les paramètres ne correspondent pas, ClickHouse ne lance pas d'exception et peut renvoyer des données incorrectes.

## Utilisation Du Tableau {#table-usage}

### Exemple {#example}

Création de la table de gauche:

``` sql
CREATE TABLE id_val(`id` UInt32, `val` UInt32) ENGINE = TinyLog
```

``` sql
INSERT INTO id_val VALUES (1,11)(2,12)(3,13)
```

Création du côté droit `Join` table:

``` sql
CREATE TABLE id_val_join(`id` UInt32, `val` UInt8) ENGINE = Join(ANY, LEFT, id)
```

``` sql
INSERT INTO id_val_join VALUES (1,21)(1,22)(3,23)
```

Rejoindre les tables:

``` sql
SELECT * FROM id_val ANY LEFT JOIN id_val_join USING (id) SETTINGS join_use_nulls = 1
```

``` text
┌─id─┬─val─┬─id_val_join.val─┐
│  1 │  11 │              21 │
│  2 │  12 │            ᴺᵁᴸᴸ │
│  3 │  13 │              23 │
└────┴─────┴─────────────────┘
```

Comme alternative, vous pouvez récupérer des données de la `Join` table, spécifiant la valeur de la clé de jointure:

``` sql
SELECT joinGet('id_val_join', 'val', toUInt32(1))
```

``` text
┌─joinGet('id_val_join', 'val', toUInt32(1))─┐
│                                         21 │
└────────────────────────────────────────────┘
```

### Sélection et insertion de données {#selecting-and-inserting-data}

Vous pouvez utiliser `INSERT` requêtes pour ajouter des données au `Join`-tables de moteur. Si la table a été créée avec `ANY` rigueur, les données pour les clés en double sont ignorées. Avec l' `ALL` rigueur, toutes les lignes sont ajoutées.

Vous ne pouvez pas effectuer un `SELECT` requête directement à partir de la table. Au lieu de cela, utilisez l'une des méthodes suivantes:

-   Placez la table sur le côté droit dans un `JOIN` clause.
-   Appelez le [joinGet](../../../sql-reference/functions/other-functions.md#joinget) fonction, qui vous permet d'extraire des données de la table de la même manière que d'un dictionnaire.

### Limitations et paramètres {#join-limitations-and-settings}

Lors de la création d'un tableau, les paramètres suivants sont appliqués:

-   [join\_use\_nulls](../../../operations/settings/settings.md#join_use_nulls)
-   [max\_rows\_in\_join](../../../operations/settings/query-complexity.md#settings-max_rows_in_join)
-   [max\_bytes\_in\_join](../../../operations/settings/query-complexity.md#settings-max_bytes_in_join)
-   [join\_overflow\_mode](../../../operations/settings/query-complexity.md#settings-join_overflow_mode)
-   [join\_any\_take\_last\_row](../../../operations/settings/settings.md#settings-join_any_take_last_row)

Le `Join`- les tables de moteur ne peuvent pas être utilisées dans `GLOBAL JOIN` opérations.

Le `Join`-moteur permet d'utiliser [join\_use\_nulls](../../../operations/settings/settings.md#join_use_nulls) réglage de la `CREATE TABLE` déclaration. Et [SELECT](../../../sql-reference/statements/select/index.md) requête permet d'utiliser `join_use_nulls` trop. Si vous avez différents `join_use_nulls` paramètres, vous pouvez obtenir une table de jointure d'erreur. Il dépend de type de JOINTURE. Lorsque vous utilisez [joinGet](../../../sql-reference/functions/other-functions.md#joinget) fonction, vous devez utiliser le même `join_use_nulls` réglage en `CRATE TABLE` et `SELECT` déclaration.

## Le Stockage De Données {#data-storage}

`Join` les données de la table sont toujours situées dans la RAM. Lors de l'insertion de lignes dans une table, ClickHouse écrit des blocs de données dans le répertoire du disque afin qu'ils puissent être restaurés lorsque le serveur redémarre.

Si le serveur redémarre incorrectement, le bloc de données sur le disque peut être perdu ou endommagé. Dans ce cas, vous devrez peut-être supprimer manuellement le fichier contenant des données endommagées.

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/join/) <!--hide-->
