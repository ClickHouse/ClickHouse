---
machine_translated: true
machine_translated_rev: cd0b14513c82d14dec07cb60cd8aabfc98681875
---

# Clause LIMIT {#limit-clause}

`LIMIT m` permet de sélectionner la première `m` lignes du résultat.

`LIMIT n, m` permet de sélectionner le `m` lignes du résultat après avoir sauté le premier `n` rangée. Le `LIMIT m OFFSET n` la syntaxe est équivalente.

`n` et `m` doivent être des entiers non négatifs.

Si il n'y a pas de [ORDER BY](order-by.md) clause qui trie explicitement les résultats, le choix des lignes pour le résultat peut être arbitraire et non déterministe.
