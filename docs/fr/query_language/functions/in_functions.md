---
machine_translated: true
---

# Fonctions pour la mise en œuvre de l'opérateur {#functions-for-implementing-the-in-operator}

## in, notin, globalIn, globalNotIn {#in-functions}

Voir la section [Dans les opérateurs](../select.md#select-in-operators).

## tuple(x, y, …), operator (x, y, …) {#tuplex-y-operator-x-y}

Une fonction qui permet de regrouper plusieurs colonnes.
For columns with the types T1, T2, …, it returns a Tuple(T1, T2, …) type tuple containing these columns. There is no cost to execute the function.
Les Tuples sont normalement utilisés comme valeurs intermédiaires pour un argument D'opérateurs IN, ou pour créer une liste de paramètres formels de fonctions lambda. Les Tuples ne peuvent pas être écrits sur une table.

## tupleElement (tuple, n), opérateur X. N {#tupleelementtuple-n-operator-x-n}

Une fonction qui permet d'obtenir une colonne à partir d'un tuple.
‘N’ est l'index de colonne, à partir de 1. N doit être une constante. ‘N’ doit être une constante. ‘N’ doit être un entier postif strict ne dépassant pas la taille du tuple.
Il n'y a aucun coût pour exécuter la fonction.

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/in_functions/) <!--hide-->
