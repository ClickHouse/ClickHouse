---
description: 'Documentation de la clause ORDER BY'
sidebar_label: 'ORDER BY'
slug: /sql-reference/statements/select/order-by
title: 'Clause ORDER BY'
doc_type: 'reference'
---

La clause `ORDER BY` contient :

* une liste d&#39;expressions, par ex. `ORDER BY visits, search_phrase`,
* une liste de nombres faisant référence aux colonnes de la clause `SELECT`, par ex. `ORDER BY 2, 1`, ou
* `ALL`, qui signifie toutes les colonnes de la clause `SELECT`, par ex. `ORDER BY ALL`.

Pour désactiver le tri par numéro de colonne, définissez le paramètre [enable&#95;positional&#95;arguments](/fr/operations/settings/settings#enable_positional_arguments) = 0.
Pour désactiver le tri par `ALL`, définissez le paramètre [enable&#95;order&#95;by&#95;all](/fr/operations/settings/settings#enable_order_by_all) = 0.

La clause `ORDER BY` peut être suivie d&#39;un modificateur `DESC` (décroissant) ou `ASC` (croissant), qui détermine le sens du tri.
Sauf si un ordre de tri explicite est indiqué, `ASC` est utilisé par défaut.
Le sens du tri s&#39;applique à une seule expression, et non à l&#39;ensemble de la liste, par ex. `ORDER BY Visits DESC, SearchPhrase`.
De plus, le tri respecte la casse.

Les lignes ayant des valeurs identiques pour une expression de tri sont renvoyées dans un ordre arbitraire et non déterministe.
Si la clause `ORDER BY` est omise dans une instruction `SELECT`, l&#39;ordre des lignes est lui aussi arbitraire et non déterministe.

<div id="sorting-of-special-values">
  ## Tri des valeurs spéciales
</div>

Il existe deux façons de définir l’ordre de tri de `NaN` et `NULL` :

* Par défaut ou avec le modificateur `NULLS LAST` : d’abord les valeurs, puis `NaN`, puis `NULL`.
* Avec le modificateur `NULLS FIRST` : d’abord `NULL`, puis `NaN`, puis les autres valeurs.

<div id="example">
  ### Exemple
</div>

Pour la table

```text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    2 │
│ 1 │  nan │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │  nan │
│ 7 │ ᴺᵁᴸᴸ │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

Exécutez la requête `SELECT * FROM t_null_nan ORDER BY y NULLS FIRST` pour obtenir :

```text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 7 │ ᴺᵁᴸᴸ │
│ 1 │  nan │
│ 6 │  nan │
│ 2 │    2 │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

Lorsque des nombres en virgule flottante sont triés, les NaN sont séparés des autres valeurs. Quel que soit l’ordre de tri, les NaN se retrouvent à la fin. Autrement dit, dans un tri croissant, ils sont placés comme s’ils étaient supérieurs à tous les autres nombres, tandis que dans un tri décroissant, ils sont placés comme s’ils étaient inférieurs aux autres.

<div id="collation-support">
  ## Prise en charge de la collation
</div>

Pour trier des valeurs [String](../../../sql-reference/data-types/string.md), vous pouvez spécifier une collation (règle de comparaison). Exemple : `ORDER BY SearchPhrase COLLATE 'tr'` — pour trier par mot-clé en ordre croissant, en utilisant l’alphabet turc, sans tenir compte de la casse, en supposant que les chaînes sont encodées en UTF-8. `COLLATE` peut être spécifié, ou non, indépendamment pour chaque expression dans ORDER BY. Si `ASC` ou `DESC` est spécifié, `COLLATE` se place après. Lors de l’utilisation de `COLLATE`, le tri est toujours insensible à la casse.

`COLLATE` est pris en charge dans [LowCardinality](../../../sql-reference/data-types/lowcardinality.md), [Nullable](../../../sql-reference/data-types/nullable.md), [Array](../../../sql-reference/data-types/array.md) et [Tuple](../../../sql-reference/data-types/tuple.md).

Nous recommandons d’utiliser `COLLATE` uniquement pour le tri final d’un petit nombre de lignes, car le tri avec `COLLATE` est moins efficace qu’un tri normal sur les octets.

<div id="collation-examples">
  ## Exemples de collation
</div>

Exemple uniquement avec des valeurs [String](../../../sql-reference/data-types/string.md) :

Table d’entrée :

```text
┌─x─┬─s────┐
│ 1 │ bca  │
│ 2 │ ABC  │
│ 3 │ 123a │
│ 4 │ abc  │
│ 5 │ BCA  │
└───┴──────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```text title="Response"
┌─x─┬─s────┐
│ 3 │ 123a │
│ 4 │ abc  │
│ 2 │ ABC  │
│ 1 │ bca  │
│ 5 │ BCA  │
└───┴──────┘
```

Exemple avec [Nullable](../../../sql-reference/data-types/nullable.md) :

Table en entrée :

```text
┌─x─┬─s────┐
│ 1 │ bca  │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │ ABC  │
│ 4 │ 123a │
│ 5 │ abc  │
│ 6 │ ᴺᵁᴸᴸ │
│ 7 │ BCA  │
└───┴──────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```text title="Response"
┌─x─┬─s────┐
│ 4 │ 123a │
│ 5 │ abc  │
│ 3 │ ABC  │
│ 1 │ bca  │
│ 7 │ BCA  │
│ 6 │ ᴺᵁᴸᴸ │
│ 2 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Exemple avec [Array](../../../sql-reference/data-types/array.md) :

Table en entrée :

```text
┌─x─┬─s─────────────┐
│ 1 │ ['Z']         │
│ 2 │ ['z']         │
│ 3 │ ['a']         │
│ 4 │ ['A']         │
│ 5 │ ['z','a']     │
│ 6 │ ['z','a','a'] │
│ 7 │ ['']          │
└───┴───────────────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```text title="Response"
┌─x─┬─s─────────────┐
│ 7 │ ['']          │
│ 3 │ ['a']         │
│ 4 │ ['A']         │
│ 2 │ ['z']         │
│ 5 │ ['z','a']     │
│ 6 │ ['z','a','a'] │
│ 1 │ ['Z']         │
└───┴───────────────┘
```

Exemple avec une chaîne [LowCardinality](../../../sql-reference/data-types/lowcardinality.md) :

Table d’entrée :

```response
┌─x─┬─s───┐
│ 1 │ Z   │
│ 2 │ z   │
│ 3 │ a   │
│ 4 │ A   │
│ 5 │ za  │
│ 6 │ zaa │
│ 7 │     │
└───┴─────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```response title="Response"
┌─x─┬─s───┐
│ 7 │     │
│ 3 │ a   │
│ 4 │ A   │
│ 2 │ z   │
│ 1 │ Z   │
│ 5 │ za  │
│ 6 │ zaa │
└───┴─────┘
```

Exemple avec [Tuple](../../../sql-reference/data-types/tuple.md) :

```response title="Response"
┌─x─┬─s───────┐
│ 1 │ (1,'Z') │
│ 2 │ (1,'z') │
│ 3 │ (1,'a') │
│ 4 │ (2,'z') │
│ 5 │ (1,'A') │
│ 6 │ (2,'Z') │
│ 7 │ (2,'A') │
└───┴─────────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```response title="Response"
┌─x─┬─s───────┐
│ 3 │ (1,'a') │
│ 5 │ (1,'A') │
│ 2 │ (1,'z') │
│ 1 │ (1,'Z') │
│ 7 │ (2,'A') │
│ 4 │ (2,'z') │
│ 6 │ (2,'Z') │
└───┴─────────┘
```

<div id="implementation-details">
  ## Détails d’implémentation
</div>

Moins de RAM est utilisée si un [LIMIT](../../../sql-reference/statements/select/limit.md) suffisamment faible est spécifié en plus de `ORDER BY`. Sinon, la quantité de mémoire consommée est proportionnelle au volume de données à trier. Pour le traitement distribué des requêtes, si [GROUP BY](/fr/sql-reference/statements/select/group-by) est omis, le tri est partiellement effectué sur les serveurs distants, puis les résultats sont fusionnés sur le serveur demandeur. Cela signifie que, pour un tri distribué, le volume de données à trier peut dépasser la quantité de mémoire disponible sur un seul serveur.

S’il n’y a pas assez de RAM, il est possible d’effectuer le tri en mémoire externe (en créant des fichiers temporaires sur un disque). Utilisez le paramètre `max_bytes_before_external_sort` à cette fin. S’il est défini sur 0 (valeur par défaut), le tri externe est désactivé. S’il est activé, lorsque le volume de données à trier atteint le nombre d’octets spécifié, les données collectées sont triées puis écrites dans un fichier temporaire. Une fois toutes les données lues, tous les fichiers triés sont fusionnés et les résultats sont renvoyés. Les fichiers sont écrits dans le répertoire `/var/lib/clickhouse/tmp/` défini dans la config (par défaut, mais vous pouvez utiliser le paramètre `tmp_path` pour modifier ce réglage). Vous pouvez également utiliser l’écriture sur disque uniquement si la requête dépasse les limites de mémoire ; par exemple, `max_bytes_ratio_before_external_sort=0.6` activera l’écriture sur disque uniquement une fois que la requête atteindra `60%` de la limite de mémoire (utilisateur/serveur).

L’exécution d’une requête peut utiliser plus de mémoire que `max_bytes_before_external_sort`. Pour cette raison, ce paramètre doit avoir une valeur nettement inférieure à `max_memory_usage`. Par exemple, si votre serveur dispose de 128 Go de RAM et que vous devez exécuter une seule requête, définissez `max_memory_usage` sur 100 Go et `max_bytes_before_external_sort` sur 80 Go.

Le tri externe est bien moins efficace que le tri en RAM.

<div id="optimization-of-data-reading">
  ## Optimisation de la lecture des données
</div>

Si l&#39;expression `ORDER BY` possède un préfixe qui correspond à la clé de tri de la table, vous pouvez optimiser la requête à l&#39;aide du paramètre [optimize&#95;read&#95;in&#95;order](../../../operations/settings/settings.md#optimize_read_in_order).

Lorsque le paramètre `optimize_read_in_order` est activé, le serveur ClickHouse utilise l&#39;index de la table et lit les données dans l&#39;ordre de la clé `ORDER BY`. Cela permet d&#39;éviter de lire toutes les données lorsqu&#39;une valeur [LIMIT](../../../sql-reference/statements/select/limit.md) est spécifiée. Ainsi, les requêtes sur de gros volumes de données avec une petite limite sont traitées plus rapidement.

L&#39;optimisation fonctionne avec `ASC` comme avec `DESC`, et ne fonctionne pas avec la clause [GROUP BY](/fr/sql-reference/statements/select/group-by) ni avec le modificateur [FINAL](/fr/sql-reference/statements/select/from#final-modifier).

Lorsque le paramètre `optimize_read_in_order` est désactivé, le serveur ClickHouse n&#39;utilise pas l&#39;index de la table lors du traitement des requêtes `SELECT`.

Envisagez de désactiver manuellement `optimize_read_in_order` lorsque vous exécutez des requêtes comportant une clause `ORDER BY`, une valeur `LIMIT` élevée et une condition [WHERE](../../../sql-reference/statements/select/where.md) qui oblige à lire un très grand nombre d&#39;enregistrements avant de trouver les données recherchées.

L&#39;optimisation est prise en charge par les moteurs de table suivants :

* [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) (y compris les [vues matérialisées](/fr/sql-reference/statements/create/view#materialized-view)),
* [Merge](../../../engines/table-engines/special/merge.md),
* [Buffer](../../../engines/table-engines/special/buffer.md)

Dans les tables utilisant le moteur `MaterializedView`, l&#39;optimisation fonctionne avec des vues telles que `SELECT ... FROM merge_tree_table ORDER BY pk`. En revanche, elle n&#39;est pas prise en charge pour des requêtes comme `SELECT ... FROM view ORDER BY pk` si la requête de la vue ne comporte pas de clause `ORDER BY`.

<div id="order-by-expr-with-fill-modifier">
  ## Modificateur ORDER BY Expr WITH FILL
</div>

Ce modificateur peut également être combiné avec le [modificateur LIMIT ... WITH TIES](/fr/sql-reference/statements/select/limit#limit--with-ties-modifier).

Le modificateur `WITH FILL` peut être utilisé après `ORDER BY expr`, avec les paramètres facultatifs `FROM expr`, `TO expr` et `STEP expr`.
Toutes les valeurs manquantes de la colonne `expr` seront générées séquentiellement, et les autres colonnes seront remplies avec leurs valeurs par défaut.

Pour remplir plusieurs colonnes, ajoutez le modificateur `WITH FILL` avec des paramètres facultatifs après chaque nom de champ dans la clause `ORDER BY`.

```sql title="Query"
ORDER BY expr [WITH FILL] [FROM const_expr] [TO const_expr] [STEP const_numeric_expr] [STALENESS const_numeric_expr], ... exprN [WITH FILL] [FROM expr] [TO expr] [STEP numeric_expr] [STALENESS numeric_expr]
[INTERPOLATE [(col [AS expr], ... colN [AS exprN])]]
```

`WITH FILL` peut être appliqué aux champs de type numérique (tous les types float, decimal et int) ou de type Date/DateTime. Lorsqu’il est appliqué à des champs `String`, les valeurs manquantes sont remplacées par des chaînes vides.
Lorsque `FROM const_expr` n’est pas défini, la séquence de remplissage utilise la valeur minimale du champ `expr` de `ORDER BY`.
Lorsque `TO const_expr` n’est pas défini, la séquence de remplissage utilise la valeur maximale du champ `expr` de `ORDER BY`.
Lorsque `STEP const_numeric_expr` est défini, `const_numeric_expr` est interprété `tel quel` pour les types numériques, en `jours` pour le type Date et en `secondes` pour le type DateTime. Il prend également en charge le type de données [INTERVAL](/fr/sql-reference/data-types/special-data-types/interval/), qui représente des intervalles de date et d’heure.
Lorsque `STEP const_numeric_expr` est omis, la séquence de remplissage utilise `1.0` pour le type numérique, `1 day` pour le type Date et `1 second` pour le type DateTime.
Lorsque `STALENESS const_numeric_expr` est défini, la requête génère des lignes jusqu’à ce que l’écart avec la ligne précédente dans les données d’origine dépasse `const_numeric_expr`.
`INTERPOLATE` peut être appliqué aux colonnes qui ne participent pas à `ORDER BY WITH FILL`. Ces colonnes sont remplies à partir des valeurs des champs précédents en appliquant `expr`. Si `expr` n’est pas présent, la valeur précédente est répétée. Si la liste est omise, toutes les colonnes autorisées sont incluses.

Exemple de requête sans `WITH FILL` :

```sql title="Query"
SELECT n, source FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n;
```

```text title="Response"
┌─n─┬─source───┐
│ 1 │ original │
│ 4 │ original │
│ 7 │ original │
└───┴──────────┘
```

Même requête après avoir appliqué le modificateur `WITH FILL` :

```sql title="Query"
SELECT n, source FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5;
```

```text title="Response"
┌───n─┬─source───┐
│   0 │          │
│ 0.5 │          │
│   1 │ original │
│ 1.5 │          │
│   2 │          │
│ 2.5 │          │
│   3 │          │
│ 3.5 │          │
│   4 │ original │
│ 4.5 │          │
│   5 │          │
│ 5.5 │          │
│   7 │ original │
└─────┴──────────┘
```

Dans le cas de plusieurs champs `ORDER BY field2 WITH FILL, field1 WITH FILL`, l’ordre de remplissage suivra l’ordre des champs dans la clause `ORDER BY`.

Exemple :

```sql title="Query"
SELECT
    toDate((number * 10) * 86400) AS d1,
    toDate(number * 86400) AS d2,
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY
    d2 WITH FILL,
    d1 WITH FILL STEP 5;
```

```text title="Response"
┌───d1───────┬───d2───────┬─source───┐
│ 1970-01-11 │ 1970-01-02 │ original │
│ 1970-01-01 │ 1970-01-03 │          │
│ 1970-01-01 │ 1970-01-04 │          │
│ 1970-02-10 │ 1970-01-05 │ original │
│ 1970-01-01 │ 1970-01-06 │          │
│ 1970-01-01 │ 1970-01-07 │          │
│ 1970-03-12 │ 1970-01-08 │ original │
└────────────┴────────────┴──────────┘
```

Le champ `d1` n’est pas complété avec la valeur par défaut, car nous n’avons pas de valeurs répétées pour `d2` et la séquence pour `d1` ne peut pas être calculée correctement.

La requête suivante avec le champ modifié dans `ORDER BY` :

```sql title="Query"
SELECT
    toDate((number * 10) * 86400) AS d1,
    toDate(number * 86400) AS d2,
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY
    d1 WITH FILL STEP 5,
    d2 WITH FILL;
```

```text title="Response"
┌───d1───────┬───d2───────┬─source───┐
│ 1970-01-11 │ 1970-01-02 │ original │
│ 1970-01-16 │ 1970-01-01 │          │
│ 1970-01-21 │ 1970-01-01 │          │
│ 1970-01-26 │ 1970-01-01 │          │
│ 1970-01-31 │ 1970-01-01 │          │
│ 1970-02-05 │ 1970-01-01 │          │
│ 1970-02-10 │ 1970-01-05 │ original │
│ 1970-02-15 │ 1970-01-01 │          │
│ 1970-02-20 │ 1970-01-01 │          │
│ 1970-02-25 │ 1970-01-01 │          │
│ 1970-03-02 │ 1970-01-01 │          │
│ 1970-03-07 │ 1970-01-01 │          │
│ 1970-03-12 │ 1970-01-08 │ original │
└────────────┴────────────┴──────────┘
```

La requête suivante utilise le type de données `INTERVAL` d’un jour pour chaque valeur remplie dans la colonne `d1` :

```sql title="Query"
SELECT
    toDate((number * 10) * 86400) AS d1,
    toDate(number * 86400) AS d2,
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY
    d1 WITH FILL STEP INTERVAL 1 DAY,
    d2 WITH FILL;
```

```response title="Response"
┌─────────d1─┬─────────d2─┬─source───┐
│ 1970-01-11 │ 1970-01-02 │ original │
│ 1970-01-12 │ 1970-01-01 │          │
│ 1970-01-13 │ 1970-01-01 │          │
│ 1970-01-14 │ 1970-01-01 │          │
│ 1970-01-15 │ 1970-01-01 │          │
│ 1970-01-16 │ 1970-01-01 │          │
│ 1970-01-17 │ 1970-01-01 │          │
│ 1970-01-18 │ 1970-01-01 │          │
│ 1970-01-19 │ 1970-01-01 │          │
│ 1970-01-20 │ 1970-01-01 │          │
│ 1970-01-21 │ 1970-01-01 │          │
│ 1970-01-22 │ 1970-01-01 │          │
│ 1970-01-23 │ 1970-01-01 │          │
│ 1970-01-24 │ 1970-01-01 │          │
│ 1970-01-25 │ 1970-01-01 │          │
│ 1970-01-26 │ 1970-01-01 │          │
│ 1970-01-27 │ 1970-01-01 │          │
│ 1970-01-28 │ 1970-01-01 │          │
│ 1970-01-29 │ 1970-01-01 │          │
│ 1970-01-30 │ 1970-01-01 │          │
│ 1970-01-31 │ 1970-01-01 │          │
│ 1970-02-01 │ 1970-01-01 │          │
│ 1970-02-02 │ 1970-01-01 │          │
│ 1970-02-03 │ 1970-01-01 │          │
│ 1970-02-04 │ 1970-01-01 │          │
│ 1970-02-05 │ 1970-01-01 │          │
│ 1970-02-06 │ 1970-01-01 │          │
│ 1970-02-07 │ 1970-01-01 │          │
│ 1970-02-08 │ 1970-01-01 │          │
│ 1970-02-09 │ 1970-01-01 │          │
│ 1970-02-10 │ 1970-01-05 │ original │
│ 1970-02-11 │ 1970-01-01 │          │
│ 1970-02-12 │ 1970-01-01 │          │
│ 1970-02-13 │ 1970-01-01 │          │
│ 1970-02-14 │ 1970-01-01 │          │
│ 1970-02-15 │ 1970-01-01 │          │
│ 1970-02-16 │ 1970-01-01 │          │
│ 1970-02-17 │ 1970-01-01 │          │
│ 1970-02-18 │ 1970-01-01 │          │
│ 1970-02-19 │ 1970-01-01 │          │
│ 1970-02-20 │ 1970-01-01 │          │
│ 1970-02-21 │ 1970-01-01 │          │
│ 1970-02-22 │ 1970-01-01 │          │
│ 1970-02-23 │ 1970-01-01 │          │
│ 1970-02-24 │ 1970-01-01 │          │
│ 1970-02-25 │ 1970-01-01 │          │
│ 1970-02-26 │ 1970-01-01 │          │
│ 1970-02-27 │ 1970-01-01 │          │
│ 1970-02-28 │ 1970-01-01 │          │
│ 1970-03-01 │ 1970-01-01 │          │
│ 1970-03-02 │ 1970-01-01 │          │
│ 1970-03-03 │ 1970-01-01 │          │
│ 1970-03-04 │ 1970-01-01 │          │
│ 1970-03-05 │ 1970-01-01 │          │
│ 1970-03-06 │ 1970-01-01 │          │
│ 1970-03-07 │ 1970-01-01 │          │
│ 1970-03-08 │ 1970-01-01 │          │
│ 1970-03-09 │ 1970-01-01 │          │
│ 1970-03-10 │ 1970-01-01 │          │
│ 1970-03-11 │ 1970-01-01 │          │
│ 1970-03-12 │ 1970-01-08 │ original │
└────────────┴────────────┴──────────┘
```

Exemple de requête sans `STALENESS` :

```sql title="Query"
SELECT number AS key, 5 * number value, 'original' AS source
FROM numbers(16) WHERE key % 5 == 0
ORDER BY key WITH FILL;
```

```text title="Response"
    ┌─key─┬─value─┬─source───┐
 1. │   0 │     0 │ original │
 2. │   1 │     0 │          │
 3. │   2 │     0 │          │
 4. │   3 │     0 │          │
 5. │   4 │     0 │          │
 6. │   5 │    25 │ original │
 7. │   6 │     0 │          │
 8. │   7 │     0 │          │
 9. │   8 │     0 │          │
10. │   9 │     0 │          │
11. │  10 │    50 │ original │
12. │  11 │     0 │          │
13. │  12 │     0 │          │
14. │  13 │     0 │          │
15. │  14 │     0 │          │
16. │  15 │    75 │ original │
    └─────┴───────┴──────────┘
```

Même requête après avoir appliqué `STALENESS 3` :

```sql title="Query"
SELECT number AS key, 5 * number value, 'original' AS source
FROM numbers(16) WHERE key % 5 == 0
ORDER BY key WITH FILL STALENESS 3;
```

```text title="Response"
    ┌─key─┬─value─┬─source───┐
 1. │   0 │     0 │ original │
 2. │   1 │     0 │          │
 3. │   2 │     0 │          │
 4. │   5 │    25 │ original │
 5. │   6 │     0 │          │
 6. │   7 │     0 │          │
 7. │  10 │    50 │ original │
 8. │  11 │     0 │          │
 9. │  12 │     0 │          │
10. │  15 │    75 │ original │
11. │  16 │     0 │          │
12. │  17 │     0 │          │
    └─────┴───────┴──────────┘
```

Exemple de requête sans `INTERPOLATE` :

```sql title="Query"
SELECT n, source, inter FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source, number AS inter
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5;
```

```text title="Response"
┌───n─┬─source───┬─inter─┐
│   0 │          │     0 │
│ 0.5 │          │     0 │
│   1 │ original │     1 │
│ 1.5 │          │     0 │
│   2 │          │     0 │
│ 2.5 │          │     0 │
│   3 │          │     0 │
│ 3.5 │          │     0 │
│   4 │ original │     4 │
│ 4.5 │          │     0 │
│   5 │          │     0 │
│ 5.5 │          │     0 │
│   7 │ original │     7 │
└─────┴──────────┴───────┘
```

Même requête après avoir appliqué `INTERPOLATE` :

```sql title="Query"
SELECT n, source, inter FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source, number AS inter
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5 INTERPOLATE (inter AS inter + 1);
```

```text title="Response"
┌───n─┬─source───┬─inter─┐
│   0 │          │     0 │
│ 0.5 │          │     0 │
│   1 │ original │     1 │
│ 1.5 │          │     2 │
│   2 │          │     3 │
│ 2.5 │          │     4 │
│   3 │          │     5 │
│ 3.5 │          │     6 │
│   4 │ original │     4 │
│ 4.5 │          │     5 │
│   5 │          │     6 │
│ 5.5 │          │     7 │
│   7 │ original │     7 │
└─────┴──────────┴───────┘
```

<div id="filling-grouped-by-sorting-prefix">
  ## Remplissage groupé par préfixe de tri
</div>

Il peut être utile de remplir séparément les lignes qui ont les mêmes valeurs dans certaines colonnes ; un bon exemple est le remplissage des valeurs manquantes dans les séries temporelles.
Supposons la table de séries temporelles suivante :

```sql
CREATE TABLE timeseries
(
    `sensor_id` UInt64,
    `timestamp` DateTime64(3, 'UTC'),
    `value` Float64
)
ENGINE = Memory;

SELECT * FROM timeseries;

┌─sensor_id─┬───────────────timestamp─┬─value─┐
│       234 │ 2021-12-01 00:00:03.000 │     3 │
│       432 │ 2021-12-01 00:00:01.000 │     1 │
│       234 │ 2021-12-01 00:00:07.000 │     7 │
│       432 │ 2021-12-01 00:00:05.000 │     5 │
└───────────┴─────────────────────────┴───────┘
```

Et nous aimerions combler les valeurs manquantes pour chaque capteur indépendamment, avec un intervalle d’une seconde.
Pour ce faire, utilisez la colonne `sensor_id` comme préfixe de tri pour remplir la colonne `timestamp` :

```sql
SELECT *
FROM timeseries
ORDER BY
    sensor_id,
    timestamp WITH FILL
INTERPOLATE ( value AS 9999 )

┌─sensor_id─┬───────────────timestamp─┬─value─┐
│       234 │ 2021-12-01 00:00:03.000 │     3 │
│       234 │ 2021-12-01 00:00:04.000 │  9999 │
│       234 │ 2021-12-01 00:00:05.000 │  9999 │
│       234 │ 2021-12-01 00:00:06.000 │  9999 │
│       234 │ 2021-12-01 00:00:07.000 │     7 │
│       432 │ 2021-12-01 00:00:01.000 │     1 │
│       432 │ 2021-12-01 00:00:02.000 │  9999 │
│       432 │ 2021-12-01 00:00:03.000 │  9999 │
│       432 │ 2021-12-01 00:00:04.000 │  9999 │
│       432 │ 2021-12-01 00:00:05.000 │     5 │
└───────────┴─────────────────────────┴───────┘
```

Ici, la colonne `value` a été interpolée avec `9999` uniquement pour rendre les lignes remplies plus visibles.
Ce comportement est contrôlé par le paramètre `use_with_fill_by_sorting_prefix` (activé par défaut)

<div id="related-content">
  ## Contenu connexe
</div>

* Blog : [Travailler avec des données de séries temporelles dans ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)