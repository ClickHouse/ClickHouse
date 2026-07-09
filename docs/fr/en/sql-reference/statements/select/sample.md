---
description: 'Documentation de la clause SAMPLE'
sidebar_label: 'SAMPLE'
slug: /sql-reference/statements/select/sample
title: 'Clause SAMPLE'
doc_type: 'reference'
---

La clause `SAMPLE` permet le traitement approximatif des requêtes `SELECT`.

Lorsque l’échantillonnage des données est activé, la requête n’est pas exécutée sur l’ensemble des données, mais seulement sur une certaine fraction de celles-ci (un échantillon). Par exemple, si vous devez calculer des statistiques sur l’ensemble des visites, il suffit d’exécuter la requête sur 1/10 de toutes les visites, puis de multiplier le résultat par 10.

Le traitement approximatif des requêtes peut être utile dans les cas suivants :

* Lorsque vous avez des exigences strictes en matière de latence (par exemple moins de 100 ms), mais que vous ne pouvez pas justifier le coût de ressources matérielles supplémentaires pour y répondre.
* Lorsque vos données brutes ne sont pas précises, de sorte que l’approximation ne dégrade pas sensiblement la qualité.
* Lorsque les exigences métier portent sur des résultats approximatifs (pour des raisons de rentabilité, ou pour réserver les résultats exacts aux utilisateurs premium).

:::note
Vous ne pouvez utiliser l’échantillonnage qu’avec les tables de la famille [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md), et uniquement si l’expression d’échantillonnage a été spécifiée lors de la création de la table (voir [moteur MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)).
:::

Les caractéristiques de l’échantillonnage des données sont les suivantes :

* L’échantillonnage des données est un mécanisme déterministe. Le résultat d’une même requête `SELECT .. SAMPLE` est toujours identique.
* L’échantillonnage fonctionne de manière cohérente sur différentes tables. Pour les tables avec une seule clé d’échantillonnage, un échantillon avec le même coefficient sélectionne toujours le même sous-ensemble des données possibles. Par exemple, un échantillon d’identifiants utilisateur sélectionne des lignes correspondant au même sous-ensemble de tous les identifiants utilisateur possibles dans différentes tables. Cela signifie que vous pouvez utiliser l’échantillon dans des sous-requêtes de la clause [IN](../../../sql-reference/operators/in.md). Vous pouvez également joindre des échantillons à l’aide de la clause [JOIN](../../../sql-reference/statements/select/join.md).
* L’échantillonnage permet de lire moins de données depuis un disque. Notez que vous devez spécifier correctement la clé d’échantillonnage. Pour plus d’informations, consultez [Creating a MergeTree Table](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table).

Pour la clause `SAMPLE`, la syntaxe suivante est prise en charge :

| Syntaxe de la clause SAMPLE | Description                                                                                                                                                                                                                                                  |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `SAMPLE k`                  | Ici, `k` est un nombre compris entre 0 et 1. La requête est exécutée sur une fraction `k` des données. Par exemple, `SAMPLE 0.1` exécute la requête sur 10 % des données. [En savoir plus](#sample-k)                                                        |
| `SAMPLE n`                  | Ici, `n` est un entier suffisamment grand. La requête est exécutée sur un échantillon d’au moins `n` lignes (mais pas beaucoup plus). Par exemple, `SAMPLE 10000000` exécute la requête sur un minimum de 10 000 000 lignes. [En savoir plus](#sample-n)     |
| `SAMPLE k OFFSET m`         | Ici, `k` et `m` sont des nombres compris entre 0 et 1. La requête est exécutée sur un échantillon représentant une fraction `k` des données. Les données utilisées pour l’échantillon sont décalées d’une fraction `m`. [En savoir plus](#sample-k-offset-m) |

<div id="sample-k">
  ## SAMPLE K
</div>

Ici, `k` est un nombre compris entre 0 et 1 (les notations fractionnaire et décimale sont toutes deux prises en charge). Par exemple, `SAMPLE 1/2` ou `SAMPLE 0.5`.

Dans une clause `SAMPLE k`, l’échantillon est prélevé sur la fraction `k` des données. Un exemple est présenté ci-dessous :

```sql
SELECT
    Title,
    count() * 10 AS PageViews
FROM hits_distributed
SAMPLE 0.1
WHERE
    CounterID = 34
GROUP BY Title
ORDER BY PageViews DESC LIMIT 1000
```

Dans cet exemple, la requête est exécutée sur un échantillon représentant 0.1 (10 %) des données. Les valeurs des fonctions d’agrégation ne sont pas corrigées automatiquement. Pour obtenir un résultat approximatif, la valeur `count()` est donc multipliée manuellement par 10.

<div id="sample-n">
  ## SAMPLE N
</div>

Ici, `n` est un entier suffisamment grand. Par exemple, `SAMPLE 10000000`.

Dans ce cas, la requête est exécutée sur un échantillon d’au moins `n` lignes (mais pas sensiblement plus). Par exemple, `SAMPLE 10000000` exécute la requête sur un minimum de 10 000 000 lignes.

Comme l’unité minimale de lecture des données est un granule (sa taille est définie par le paramètre `index_granularity`), il est judicieux de définir un échantillon bien plus grand que la taille du granule.

Lorsque vous utilisez la clause `SAMPLE n`, vous ne savez pas quel pourcentage relatif des données a été traité. Vous ne connaissez donc pas le coefficient par lequel les fonctions d’agrégation doivent être multipliées. Utilisez la colonne virtuelle `_sample_factor` pour obtenir un résultat approximatif.

La colonne `_sample_factor` contient des coefficients relatifs calculés dynamiquement. Cette colonne est créée automatiquement lorsque vous [créez](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) une table avec la clé d’échantillonnage spécifiée. Les exemples d’utilisation de la colonne `_sample_factor` sont présentés ci-dessous.

Considérons la table `visits`, qui contient des statistiques sur les visites du site. Le premier exemple montre comment calculer le nombre de pages vues :

```sql
SELECT sum(PageViews * _sample_factor)
FROM visits
SAMPLE 10000000
```

L’exemple suivant montre comment calculer le nombre total de visites :

```sql
SELECT sum(_sample_factor)
FROM visits
SAMPLE 10000000
```

L&#39;exemple ci-dessous montre comment calculer la durée moyenne d&#39;une session. Notez que vous n&#39;avez pas besoin d&#39;utiliser le coefficient relatif pour calculer les valeurs moyennes.

```sql
SELECT avg(Duration)
FROM visits
SAMPLE 10000000
```

<div id="sample-k-offset-m">
  ## SAMPLE K OFFSET M
</div>

Ici, `k` et `m` sont des nombres entre 0 et 1. Des exemples sont présentés ci-dessous.

**Exemple 1**

```sql
SAMPLE 1/10
```

Dans cet exemple, l’échantillon correspond à 1/10 de l’ensemble des données :

`[++------------]`

**Exemple 2**

```sql
SAMPLE 1/10 OFFSET 1/2
```

Ici, un échantillon de 10 % est pris dans la seconde moitié des données.

`[------++------]`