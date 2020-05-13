---
machine_translated: true
machine_translated_rev: cd0b14513c82d14dec07cb60cd8aabfc98681875
---

# Dans OUTFILE Clause {#into-outfile-clause}

Ajouter l' `INTO OUTFILE filename` clause (où filename est un littéral de chaîne) pour `SELECT query` pour rediriger sa sortie vers le fichier spécifié côté client.

## Détails De Mise En Œuvre {#implementation-details}

-   Cette fonctionnalité est disponible dans les [client de ligne de commande](../../../interfaces/cli.md) et [clickhouse-local](../../../operations/utilities/clickhouse-local.md). Ainsi, une requête envoyée par [Interface HTTP](../../../interfaces/http.md) va échouer.
-   La requête échouera si un fichier portant le même nom existe déjà.
-   Défaut [le format de sortie](../../../interfaces/formats.md) être `TabSeparated` (comme dans le mode batch client en ligne de commande).
