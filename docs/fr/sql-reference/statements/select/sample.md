---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# Exemple de Clause {#select-sample-clause}

Le `SAMPLE` clause permet approchée `SELECT` le traitement de la requête.

Lorsque l'échantillonnage de données est activé, la requête n'est pas effectuée sur toutes les données, mais uniquement sur une certaine fraction de données (échantillon). Par exemple, si vous avez besoin de calculer des statistiques pour toutes les visites, il suffit d'exécuter la requête sur le 1/10 de la fraction de toutes les visites, puis multiplier le résultat par 10.

Le traitement approximatif des requêtes peut être utile dans les cas suivants:

-   Lorsque vous avez des exigences de synchronisation strictes (comme \<100ms), mais que vous ne pouvez pas justifier le coût des ressources matérielles supplémentaires pour y répondre.
-   Lorsque vos données brutes ne sont pas précises, l'approximation ne dégrade pas sensiblement la qualité.
-   Les exigences commerciales ciblent des résultats approximatifs (pour la rentabilité, ou pour commercialiser des résultats exacts aux utilisateurs premium).

!!! note "Note"
    Vous ne pouvez utiliser l'échantillonnage qu'avec les tables [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) famille, et seulement si l'expression d'échantillonnage a été spécifiée lors de la création de la table (voir [Moteur MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)).

Les caractéristiques de l'échantillonnage des données sont énumérées ci-dessous:

-   L'échantillonnage de données est un mécanisme déterministe. Le résultat de la même `SELECT .. SAMPLE` la requête est toujours le même.
-   L'échantillonnage fonctionne de manière cohérente pour différentes tables. Pour les tables avec une seule clé d'échantillonnage, un échantillon avec le même coefficient sélectionne toujours le même sous-ensemble de données possibles. Par exemple, un exemple d'ID utilisateur prend des lignes avec le même sous-ensemble de tous les ID utilisateur possibles de différentes tables. Cela signifie que vous pouvez utiliser l'exemple dans les sous-requêtes dans la [IN](../../operators/in.md) clause. En outre, vous pouvez joindre des échantillons en utilisant le [JOIN](join.md) clause.
-   L'échantillonnage permet de lire moins de données à partir d'un disque. Notez que vous devez spécifier l'échantillonnage clé correctement. Pour plus d'informations, voir [Création d'une Table MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table).

Pour l' `SAMPLE` clause la syntaxe suivante est prise en charge:

| SAMPLE Clause Syntax | Description                                                                                                                                                                                                                                                                 |
|----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SAMPLE k`           | Ici `k` est le nombre de 0 à 1.</br>La requête est exécutée sur `k` fraction des données. Exemple, `SAMPLE 0.1` exécute la requête sur 10% des données. [Lire plus](#select-sample-k)                                                                                       |
| `SAMPLE n`           | Ici `n` est un entier suffisamment grand.</br>La requête est exécutée sur un échantillon d'au moins `n` lignes (mais pas significativement plus que cela). Exemple, `SAMPLE 10000000` exécute la requête sur un minimum de 10 000 000 lignes. [Lire plus](#select-sample-n) |
| `SAMPLE k OFFSET m`  | Ici `k` et `m` sont les nombres de 0 à 1.</br>La requête est exécutée sur un échantillon de `k` fraction des données. Les données utilisées pour l'échantillon est compensée par `m` fraction. [Lire plus](#select-sample-offset)                                           |

## SAMPLE K {#select-sample-k}

Ici `k` est le nombre de 0 à 1 (les notations fractionnaires et décimales sont prises en charge). Exemple, `SAMPLE 1/2` ou `SAMPLE 0.5`.

Dans un `SAMPLE k` clause, l'échantillon est prélevé à partir de la `k` fraction des données. L'exemple est illustré ci-dessous:

``` sql
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

Dans cet exemple, la requête est exécutée sur un échantillon de 0,1 (10%) de données. Les valeurs des fonctions d'agrégat ne sont pas corrigées automatiquement, donc pour obtenir un résultat approximatif, la valeur `count()` est multiplié manuellement par 10.

## SAMPLE N {#select-sample-n}

Ici `n` est un entier suffisamment grand. Exemple, `SAMPLE 10000000`.

Dans ce cas, la requête est exécutée sur un échantillon d'au moins `n` lignes (mais pas significativement plus que cela). Exemple, `SAMPLE 10000000` exécute la requête sur un minimum de 10 000 000 lignes.

Puisque l'unité minimale pour la lecture des données est un granule (sa taille est définie par le `index_granularity` de réglage), il est logique de définir un échantillon beaucoup plus grand que la taille du granule.

Lors de l'utilisation de la `SAMPLE n` clause, vous ne savez pas quel pourcentage relatif de données a été traité. Donc, vous ne connaissez pas le coefficient par lequel les fonctions agrégées doivent être multipliées. L'utilisation de la `_sample_factor` colonne virtuelle pour obtenir le résultat approximatif.

Le `_sample_factor` colonne contient des coefficients relatifs qui sont calculés dynamiquement. Cette colonne est créée automatiquement lorsque vous [créer](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) une table avec la clé d'échantillonnage spécifiée. Les exemples d'utilisation de la `_sample_factor` colonne sont indiqués ci-dessous.

Considérons la table `visits` qui contient des statistiques sur les visites de site. Le premier exemple montre comment calculer le nombre de pages vues:

``` sql
SELECT sum(PageViews * _sample_factor)
FROM visits
SAMPLE 10000000
```

L'exemple suivant montre comment calculer le nombre total de visites:

``` sql
SELECT sum(_sample_factor)
FROM visits
SAMPLE 10000000
```

L'exemple ci-dessous montre comment calculer la durée moyenne de la session. Notez que vous n'avez pas besoin d'utiliser le coefficient relatif pour calculer les valeurs moyennes.

``` sql
SELECT avg(Duration)
FROM visits
SAMPLE 10000000
```

## SAMPLE K OFFSET M {#select-sample-offset}

Ici `k` et `m` sont des nombres de 0 à 1. Des exemples sont présentés ci-dessous.

**Exemple 1**

``` sql
SAMPLE 1/10
```

Dans cet exemple, l'échantillon représente 1 / 10e de toutes les données:

`[++------------]`

**Exemple 2**

``` sql
SAMPLE 1/10 OFFSET 1/2
```

Ici, un échantillon de 10% est prélevé à partir de la seconde moitié des données.

`[------++------]`
