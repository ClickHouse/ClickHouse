---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 60
toc_title: "Mise en \u0153uvre de L'op\xE9rateur IN"
---

# Fonctions de mise en œuvre de L'opérateur IN {#functions-for-implementing-the-in-operator}

## in, notin, globalIn, globalNotIn {#in-functions}

Voir la section [Dans les opérateurs](../operators/in.md#select-in-operators).

## tuple(x, y, …), operator (x, y, …) {#tuplex-y-operator-x-y}

Une fonction qui permet de regrouper plusieurs colonnes.
For columns with the types T1, T2, …, it returns a Tuple(T1, T2, …) type tuple containing these columns. There is no cost to execute the function.
Les Tuples sont normalement utilisés comme valeurs intermédiaires pour un argument D'opérateurs IN, ou pour créer une liste de paramètres formels de fonctions lambda. Les Tuples ne peuvent pas être écrits sur une table.

## tupleElement (tuple, n), opérateur X. N {#tupleelementtuple-n-operator-x-n}

Une fonction qui permet d'obtenir une colonne à partir d'un tuple.
‘N’ est l'index de colonne, à partir de 1. N doit être une constante. ‘N’ doit être une constante. ‘N’ doit être un entier postif strict ne dépassant pas la taille du tuple.
Il n'y a aucun coût pour exécuter la fonction.

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/in_functions/) <!--hide-->
