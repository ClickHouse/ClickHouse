---
description: 'Documentation de l’optimisation LowCardinality pour les colonnes de type String'
sidebar_label: 'LowCardinality(T)'
sidebar_position: 42
slug: /sql-reference/data-types/lowcardinality
title: 'LowCardinality(T)'
doc_type: 'reference'
---

Modifie la représentation interne d’autres types de données en encodage par dictionnaire.

<div id="syntax">
  ## Syntaxe
</div>

```sql
LowCardinality(data_type)
```

**Paramètres**

* `data_type` — [String](../../sql-reference/data-types/string.md), [FixedString](../../sql-reference/data-types/fixedstring.md), [Date](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md), ainsi que les nombres, à l&#39;exception de [Decimal](../../sql-reference/data-types/decimal.md). `LowCardinality` n&#39;est pas performant avec certains types de données ; voir la description du paramètre [allow&#95;suspicious&#95;low&#95;cardinality&#95;types](../../operations/settings/settings.md#allow_suspicious_low_cardinality_types).

<div id="description">
  ## Description
</div>

`LowCardinality` est une surcouche qui modifie la méthode de stockage des données et les règles de traitement des données. ClickHouse applique un [encodage par dictionnaire](https://en.wikipedia.org/wiki/Dictionary_coder) aux colonnes `LowCardinality`. L’utilisation de données encodées par dictionnaire augmente considérablement les performances des requêtes [SELECT](../../sql-reference/statements/select/index.md) dans de nombreuses applications.

L’efficacité du type de données `LowCardinality` dépend de la diversité des données. Si un dictionnaire contient moins de 10 000 valeurs distinctes, ClickHouse offre généralement de meilleures performances en lecture et en stockage des données. Si un dictionnaire contient plus de 100 000 valeurs distinctes, ClickHouse peut en revanche être moins performant qu’avec des types de données ordinaires.

Envisagez d’utiliser `LowCardinality` à la place de [Enum](../../sql-reference/data-types/enum.md) lorsque vous travaillez avec des chaînes de caractères. `LowCardinality` offre davantage de souplesse et présente souvent une efficacité égale, voire supérieure.

<div id="example">
  ## Exemple
</div>

Créez une table avec une colonne `LowCardinality` :

```sql
CREATE TABLE lc_t
(
    `id` UInt16,
    `strings` LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY id
```

<div id="related-settings-and-functions">
  ## Paramètres et fonctions associés
</div>

Paramètres :

* [low&#95;cardinality&#95;max&#95;dictionary&#95;size](../../operations/settings/settings.md#low_cardinality_max_dictionary_size)
* [low&#95;cardinality&#95;use&#95;single&#95;dictionary&#95;for&#95;part](../../operations/settings/settings.md#low_cardinality_use_single_dictionary_for_part)
* [low&#95;cardinality&#95;allow&#95;in&#95;native&#95;format](../../operations/settings/settings.md#low_cardinality_allow_in_native_format)
* [allow&#95;suspicious&#95;low&#95;cardinality&#95;types](../../operations/settings/settings.md#allow_suspicious_low_cardinality_types)
* [output&#95;format&#95;arrow&#95;low&#95;cardinality&#95;as&#95;dictionary](/fr/operations/settings/formats#output_format_arrow_low_cardinality_as_dictionary)

Fonctions :

* [toLowCardinality](../../sql-reference/functions/type-conversion-functions.md#toLowCardinality)

<div id="related-content">
  ## Contenu connexe
</div>

* Blog : [Optimiser ClickHouse avec des schémas et des codecs](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
* Blog : [Utiliser des données de séries temporelles dans ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
* [Optimisation de String (présentation vidéo en russe)](https://youtu.be/rqf-ILRgBdY?list=PL0Z2YDlm0b3iwXCpEFiOOYmwXzVmjJfEt). [Diapositives en anglais](https://github.com/ClickHouse/clickhouse-presentations/raw/master/meetup19/string_optimization.pdf)