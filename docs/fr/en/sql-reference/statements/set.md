---
description: 'Documentation de l’instruction SET'
sidebar_label: 'SET'
sidebar_position: 50
slug: /sql-reference/statements/set
title: 'Instruction SET'
doc_type: 'reference'
---

```sql
SET param = value
```

Affecte `value` au [paramètre](/fr/operations/settings/overview) `param` pour la session en cours. Vous ne pouvez pas modifier les [paramètres du serveur](../../operations/server-configuration-parameters/settings.md) de cette manière.

Vous pouvez également définir, dans une seule requête, toutes les valeurs du profil de paramètres spécifié.

```sql
SET profile = 'profile-name-from-the-settings-file'
```

Pour les paramètres booléens définis sur `true`, vous pouvez utiliser une syntaxe abrégée en omettant l’affectation de la valeur. Lorsque seul le nom du paramètre est indiqué, il est automatiquement défini sur `1` (`true`).

```sql
-- These are equivalent:
SET force_index_by_date = 1
SET force_index_by_date
```

<div id="set-time-zone">
  ## SET TIME ZONE
</div>

```sql
SET TIME ZONE [=] 'timezone'
```

Définit le fuseau horaire de la session. Il s’agit d’un alias de `SET session_timezone = 'timezone'`, fourni pour assurer la compatibilité avec PostgreSQL et d’autres bases de données SQL.

De nombreux clients SQL, ORM et pilotes JDBC envoient automatiquement `SET TIME ZONE` lors de la connexion. Cette syntaxe permet à ces outils de fonctionner avec ClickHouse sans recourir à des solutions de contournement personnalisées.

```sql
SET TIME ZONE 'UTC';
SET TIME ZONE 'Europe/Amsterdam';
SET TIME ZONE 'America/New_York';

-- Verify the current session time zone
SELECT getSetting('session_timezone');
```

La valeur de timezone doit correspondre à un nom valide de la [IANA Time Zone Database](https://www.iana.org/time-zones). Un nom de timezone non valide entraînera une erreur.

Pour plus d’informations sur le paramètre `session_timezone`, consultez [session&#95;timezone](/fr/operations/settings/settings#session_timezone).

<div id="setting-query-parameters">
  ## Définir des paramètres de requête
</div>

L’instruction `SET` peut également être utilisée pour définir des paramètres de requête en préfixant le nom du paramètre par `param_`.
Les paramètres de requête vous permettent d’écrire des requêtes génériques avec des marqueurs de substitution, qui sont remplacés par des valeurs réelles au moment de l’exécution.

```sql
SET param_name = value
```

Pour utiliser un paramètre de requête dans votre requête, faites-y référence avec la syntaxe `{name: datatype}` :

```sql
SET param_id = 42;
SET param_name = 'John';

SELECT * FROM users
WHERE id = {id: UInt32}
AND name = {name: String};
```

Les paramètres de requête sont particulièrement utiles lorsque la même requête doit être exécutée plusieurs fois avec des valeurs différentes.

Pour des informations plus détaillées sur les paramètres de requête, notamment leur utilisation avec le type `Identifier`, consultez [Définir et utiliser des paramètres de requête](../../sql-reference/syntax.md#defining-and-using-query-parameters).

Pour en savoir plus, consultez [Paramètres](../../operations/settings/settings.md).