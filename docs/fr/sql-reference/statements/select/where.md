---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# Clause where {#select-where}

`WHERE` clause permet de filtrer les données en provenance de [FROM](from.md) la clause de `SELECT`.

Si il y a un `WHERE` , il doit contenir une expression avec la `UInt8` type. C'est généralement une expression avec comparaison et opérateurs logiques. Les lignes où cette expression est évaluée à 0 sont exclues des transformations ou des résultats ultérieurs.

`WHERE` expression est évaluée sur la possibilité d'utiliser des index et l'élagage de partition, si le moteur de table sous-jacent le prend en charge.

!!! note "Note"
    Il y a une optimisation de filtrage appelée [prewhere](prewhere.md).
