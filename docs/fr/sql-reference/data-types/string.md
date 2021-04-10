---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: "Cha\xEEne"
---

# Chaîne {#string}

Les chaînes d'une longueur arbitraire. La longueur n'est pas limitée. La valeur peut contenir un ensemble arbitraire d'octets, y compris des octets nuls.
Le type de chaîne remplace les types VARCHAR, BLOB, CLOB et autres provenant d'autres SGBD.

## Encodage {#encodings}

ClickHouse n'a pas le concept d'encodages. Les chaînes peuvent contenir un ensemble arbitraire d'octets, qui sont stockés et sortis tels quels.
Si vous avez besoin de stocker des textes, nous vous recommandons d'utiliser L'encodage UTF-8. À tout le moins, si votre terminal utilise UTF-8 (comme recommandé), vous pouvez lire et écrire vos valeurs sans effectuer de conversions.
De même, certaines fonctions pour travailler avec des chaînes ont des variations distinctes qui fonctionnent sous l'hypothèse que la chaîne contient un ensemble d'octets représentant un texte codé en UTF-8.
Par exemple, l' ‘length’ fonction calcule la longueur de la chaîne en octets, tandis que le ‘lengthUTF8’ la fonction calcule la longueur de la chaîne en points de code Unicode, en supposant que la valeur est encodée en UTF-8.

[Article Original](https://clickhouse.tech/docs/en/data_types/string/) <!--hide-->
