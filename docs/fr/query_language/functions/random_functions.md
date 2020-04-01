---
machine_translated: true
---

# Fonctions pour générer des nombres pseudo-aléatoires {#functions-for-generating-pseudo-random-numbers}

Des générateurs Non cryptographiques de nombres pseudo-aléatoires sont utilisés.

Toutes les fonctions acceptent zéro argument ou un argument.
Si un argument est passé, il peut être de n'importe quel type, et sa valeur n'est utilisée pour rien.
Le seul but de cet argument est d'empêcher l'élimination des sous-expressions courantes, de sorte que deux instances différentes de la même fonction renvoient des colonnes différentes avec des nombres aléatoires différents.

## Rand {#rand}

Renvoie un nombre UInt32 pseudo-aléatoire, réparti uniformément entre tous les nombres de type UInt32.
Utilise un générateur congruentiel linéaire.

## rand64 {#rand64}

Renvoie un nombre UInt64 pseudo-aléatoire, réparti uniformément entre tous les nombres de type UInt64.
Utilise un générateur congruentiel linéaire.

## randConstant {#randconstant}

Renvoie un nombre UInt32 pseudo-aléatoire, la valeur est une pour différents blocs.

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/random_functions/) <!--hide-->
