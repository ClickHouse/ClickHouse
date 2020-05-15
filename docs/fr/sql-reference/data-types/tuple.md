---
machine_translated: true
machine_translated_rev: f865c9653f9df092694258e0ccdd733c339112f5
toc_priority: 53
toc_title: Tuple (T1, T2,...)
---

# Tuple(t1, T2, …) {#tuplet1-t2}

Un n-uplet d’éléments, chacun ayant une personne [type](index.md#data_types).

Les Tuples sont utilisés pour le regroupement temporaire de colonnes. Les colonnes peuvent être regroupées lorsqu’une expression IN est utilisée dans une requête et pour spécifier certains paramètres formels des fonctions lambda. Pour plus d’informations, voir les sections [Dans les opérateurs](../../sql-reference/statements/select.md) et [Des fonctions d’ordre supérieur](../../sql-reference/functions/higher-order-functions.md).

Les Tuples peuvent être le résultat d’une requête. Dans ce cas, pour les formats de texte autres que JSON, les valeurs sont séparées par des virgules entre parenthèses. Dans les formats JSON, les tuples sont sortis sous forme de tableaux (entre crochets).

## La création d’un Tuple {#creating-a-tuple}

Vous pouvez utiliser une fonction pour créer un tuple:

``` sql
tuple(T1, T2, ...)
```

Exemple de création d’un tuple:

``` sql
SELECT tuple(1,'a') AS x, toTypeName(x)
```

``` text
┌─x───────┬─toTypeName(tuple(1, 'a'))─┐
│ (1,'a') │ Tuple(UInt8, String)      │
└─────────┴───────────────────────────┘
```

## Utilisation De Types De Données {#working-with-data-types}

Lors de la création d’un tuple à la volée, ClickHouse détecte automatiquement le type de chaque argument comme le minimum des types qui peuvent stocker la valeur de l’argument. Si l’argument est [NULL](../../sql-reference/syntax.md#null-literal) le type de l’élément tuple est [Nullable](nullable.md).

Exemple de détection automatique de type de données:

``` sql
SELECT tuple(1, NULL) AS x, toTypeName(x)
```

``` text
┌─x────────┬─toTypeName(tuple(1, NULL))──────┐
│ (1,NULL) │ Tuple(UInt8, Nullable(Nothing)) │
└──────────┴─────────────────────────────────┘
```

[Article Original](https://clickhouse.tech/docs/en/data_types/tuple/) <!--hide-->
