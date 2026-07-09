---
description: 'Documentation sur les types de données en virgule flottante dans ClickHouse : Float32,
  Float64 et BFloat16'
sidebar_label: 'Float32 | Float64 | BFloat16'
sidebar_position: 4
slug: /sql-reference/data-types/float
title: 'Types Float32 | Float64 | BFloat16'
doc_type: 'reference'
---

:::note
Si vous avez besoin de calculs exacts, en particulier si vous travaillez avec des données financières ou commerciales exigeant une grande précision, vous devriez envisager d&#39;utiliser [Decimal](../data-types/decimal.md) à la place.

Les [nombres à virgule flottante](https://en.wikipedia.org/wiki/IEEE_754) peuvent produire des résultats inexacts, comme illustré ci-dessous :

```sql
CREATE TABLE IF NOT EXISTS float_vs_decimal
(
   my_float Float64,
   my_decimal Decimal64(3)
)
ENGINE=MergeTree
ORDER BY tuple();

# Generate 1 000 000 random numbers with 2 decimal places and store them as a float and as a decimal
INSERT INTO float_vs_decimal SELECT round(randCanonical(), 3) AS res, res FROM system.numbers LIMIT 1000000;
```

```sql
SELECT sum(my_float), sum(my_decimal) FROM float_vs_decimal;

┌──────sum(my_float)─┬─sum(my_decimal)─┐
│ 499693.60500000004 │      499693.605 │
└────────────────────┴─────────────────┘

SELECT sumKahan(my_float), sumKahan(my_decimal) FROM float_vs_decimal;

┌─sumKahan(my_float)─┬─sumKahan(my_decimal)─┐
│         499693.605 │           499693.605 │
└────────────────────┴──────────────────────┘
```

:::

Les types équivalents dans ClickHouse et en langage C sont indiqués ci-dessous :

* `Float32` — `float`.
* `Float64` — `double`.

Les types Float dans ClickHouse ont les alias suivants :

* `Float32` — `FLOAT`, `REAL`, `SINGLE`.
* `Float64` — `DOUBLE`, `DOUBLE PRECISION`.

Lors de la création de tables, il est possible de spécifier des paramètres numériques pour les nombres à virgule flottante (par ex. `FLOAT(12)`, `FLOAT(15, 22)`, `DOUBLE(12)`, `DOUBLE(4, 18)`), mais ClickHouse les ignore.

<div id="using-floating-point-numbers">
  ## Utilisation des nombres à virgule flottante
</div>

* Les calculs avec des nombres à virgule flottante peuvent entraîner une erreur d&#39;arrondi.

{/* */ }

```sql
SELECT 1 - 0.9

┌───────minus(1, 0.9)─┐
│ 0.09999999999999998 │
└─────────────────────┘
```

* Le résultat du calcul dépend de la méthode de calcul (du type de processeur et de l’architecture du système).
* Les calculs en virgule flottante peuvent produire des valeurs telles que l’infini (`Inf`) et « pas un nombre » (`NaN`). Il convient d’en tenir compte lors du traitement des résultats des calculs.
* Lors de l’analyse de nombres à virgule flottante à partir d’un texte, le résultat peut ne pas correspondre au nombre représentable par la machine le plus proche.

<div id="nan-and-inf">
  ## NaN et Inf
</div>

Contrairement au SQL standard, ClickHouse prend en charge les catégories suivantes de nombres à virgule flottante :

* `Inf` – Infini.

{/* */ }

```sql
SELECT 0.5 / 0

┌─divide(0.5, 0)─┐
│            inf │
└────────────────┘
```

* `-Inf` — Infini négatif.

{/* */ }

```sql
SELECT -0.5 / 0

┌─divide(-0.5, 0)─┐
│            -inf │
└─────────────────┘
```

* `NaN` — n’est pas un nombre.

{/* */ }

```sql
SELECT 0 / 0

┌─divide(0, 0)─┐
│          nan │
└──────────────┘
```

Voir les règles de tri de `NaN` dans la section [clause ORDER BY](../../sql-reference/statements/select/order-by.md).

<div id="nan-values-in-set-semantics">
  ## Valeurs NaN dans la sémantique des ensembles
</div>

La norme IEEE 754 définit `NaN` de telle sorte que la comparaison scalaire `NaN = NaN` renvoie `false`.
ClickHouse suit cette règle pour l’opérateur `=`.

Cependant, `NaN` n’est pas une valeur unique ; c’est n’importe quel motif de bits dont l’exposant est constitué uniquement de 1 et dont la
mantisse est non nulle. Des opérations différentes et des architectures CPU différentes peuvent produire des valeurs `NaN`
avec des bits de signe différents ou des payloads de mantisse différents. Par exemple :

* `0./0.` produit un `NaN` dont le bit de signe vaut 1 sur la plupart des plateformes x86.
* Le littéral `nan` produit un `NaN` dont le bit de signe vaut 0.
* Après [PR #98230](https://github.com/ClickHouse/ClickHouse/pull/98230), le chemin AArch64 NEON de
  `log` renvoie un `NaN` dont le bit de signe diffère de celui du `log` scalaire de glibc pour des entrées négatives.

Les tables de hachage dans ClickHouse comparent les clés octet par octet. Des motifs de bits `NaN` différents sont donc hachés dans
des compartiments différents et traités comme des valeurs distinctes par les opérations à sémantique d’ensemble, notamment
`DISTINCT`, `GROUP BY`, `uniqExact`, `countDistinct` et les equi-`JOIN` sur une clé `Float` :

```sql
SELECT countDistinct(arrayJoin([0./0., nan, log(-1.)]));
-- May return 2 or 3 depending on architecture and build, even though all three inputs are NaN.
```

Ceci est conforme à la norme IEEE 754 (chaque `NaN` est différent de toute autre valeur, y compris de lui-même),
mais cela peut être surprenant. Si vous avez besoin que les opérations avec une sémantique d’ensemble traitent toutes les valeurs `NaN` comme égales,
normalisez-les dans la requête :

```sql
-- Replace every NaN with a single canonical NaN value
SELECT countDistinct(if(isNaN(x), CAST('nan' AS Float64), x))
FROM (SELECT arrayJoin([0./0., nan, log(-1.)]) AS x);
-- Returns 1.

-- Or exclude NaN values from the set entirely
SELECT countDistinct(if(isNaN(x), NULL, x))
FROM (SELECT arrayJoin([0./0., nan, log(-1.)]) AS x);
-- Returns 0.
```

La même approche s’applique aux clés `DISTINCT`, `GROUP BY` et `JOIN`.

<div id="bfloat16">
  ## BFloat16
</div>

`BFloat16` est un type de données en virgule flottante sur 16 bits, avec un exposant sur 8 bits, un bit de signe et une mantisse sur 7 bits.
Il est utile pour les applications de machine learning et d’IA.

ClickHouse prend en charge les conversions entre `Float32` et `BFloat16`, qui
peuvent être effectuées à l’aide des fonctions [`toFloat32()`](../functions/type-conversion-functions.md/#toFloat32) ou [`toBFloat16`](../functions/type-conversion-functions.md/#toBFloat16).

:::note
La plupart des autres opérations ne sont pas prises en charge.
:::