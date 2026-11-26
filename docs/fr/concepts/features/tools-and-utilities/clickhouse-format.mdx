---
description: 'Guide d’utilisation de l’utilitaire format pour travailler avec les formats de données ClickHouse'
slug: /operations/utilities/clickhouse-format
title: 'clickhouse-format'
doc_type: 'reference'
---

Permet de mettre en forme les requêtes en entrée.

Options :

* `--help` or`-h` — Affiche un message d’aide.
* `--query` — Met en forme des requêtes de toute longueur et complexité.
* `--hilite` or `--highlight` — Ajoute une coloration syntaxique avec des séquences d’échappement ANSI pour le terminal.
* `--oneline` — Met en forme sur une seule ligne.
* `--max_line_length` — Met en forme sur une seule ligne les requêtes dont la longueur est inférieure à la valeur spécifiée.
* `--comments` — Conserve les commentaires dans la sortie.
* `--quiet` or `-q` — Vérifie uniquement la syntaxe, sans sortie en cas de succès.
* `--multiquery` or `-n` — Autorise plusieurs requêtes dans le même fichier.
* `--obfuscate` — Obfusque au lieu de mettre en forme.
* `--seed <string>` — Seed arbitraire qui détermine le résultat de l’obfuscation.
* `--backslash` — Ajoute un backslash à la fin de chaque ligne de la requête mise en forme. Cela peut être utile lorsque vous copiez une requête depuis le Web ou ailleurs sur plusieurs lignes et souhaitez l’exécuter en ligne de commande.
* `--semicolons_inline` — En mode multiquery, écrit les points-virgules sur la dernière ligne de la requête au lieu de les placer sur une nouvelle ligne.

<div id="examples">
  ## Exemples
</div>

1. Mise en forme d’une requête :

```bash title="Query"
$ clickhouse-format --query "select number from numbers(10) where number%2 order by number desc;"
```

```bash title="Response"
SELECT number
FROM numbers(10)
WHERE number % 2
ORDER BY number DESC
```

2. Mise en évidence et format sur une seule ligne :

```bash title="Query"
$ clickhouse-format --oneline --hilite <<< "SELECT sum(number) FROM numbers(5);"
```

```sql title="Response"
SELECT sum(number) FROM numbers(5)
```

3. Requêtes multiples :

```bash title="Query"
$ clickhouse-format -n <<< "SELECT min(number) FROM numbers(5); SELECT max(number) FROM numbers(5);"
```

```sql title="Response"
SELECT min(number)
FROM numbers(5)
;

SELECT max(number)
FROM numbers(5)
;

```

4. Obfuscation :

```bash title="Query"
$ clickhouse-format --seed Hello --obfuscate <<< "SELECT cost_first_screen BETWEEN a AND b, CASE WHEN x >= 123 THEN y ELSE NULL END;"
```

```sql title="Response"
SELECT treasury_mammoth_hazelnut BETWEEN nutmeg AND span, CASE WHEN chive >= 116 THEN switching ELSE ANYTHING END;
```

Même requête avec une autre chaîne de seed :

```bash title="Query"
$ clickhouse-format --seed World --obfuscate <<< "SELECT cost_first_screen BETWEEN a AND b, CASE WHEN x >= 123 THEN y ELSE NULL END;"
```

```sql title="Response"
SELECT horse_tape_summer BETWEEN folklore AND moccasins, CASE WHEN intestine >= 116 THEN nonconformist ELSE FORESTRY END;
```

5. Ajout d’un backslash :

```bash title="Query"
$ clickhouse-format --backslash <<< "SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION DISTINCT SELECT 3);"
```

```sql title="Response"
SELECT * \
FROM  \
( \
    SELECT 1 AS x \
    UNION ALL \
    SELECT 1 \
    UNION DISTINCT \
    SELECT 3 \
)
```