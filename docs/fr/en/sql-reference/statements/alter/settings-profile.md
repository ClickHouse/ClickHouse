---
description: 'Documentation de SETTINGS PROFILE'
sidebar_label: 'SETTINGS PROFILE'
sidebar_position: 48
slug: /sql-reference/statements/alter/settings-profile
title: 'ALTER SETTINGS PROFILE'
doc_type: 'reference'
---

Modifie les profils de paramètres.

Syntaxe :

```sql
ALTER SETTINGS PROFILE [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]]
    [ON CLUSTER cluster_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | INHERIT 'profile_name'] [,...]
    [ADD|MODIFY SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...]
    [SET variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...] ]
    [DROP SETTINGS variable [,...] ]
    [ADD PROFILES 'profile_name' [,...] ]
    [DROP PROFILES 'profile_name' [,...] ]
    [DROP ALL SETTINGS]
    [DROP ALL PROFILES]
    [TO {{role1 | user1 [, role2 | user2 ...]} | NONE | ALL | ALL EXCEPT {role1 | user1 [, role2 | user2 ...]}}]
```

La clause `ON CLUSTER` permet de modifier les profils de paramètres au sein d’un cluster ; voir [Distributed DDL](../../../sql-reference/distributed-ddl.md).

<div id="replacing-vs-modifying">
  ## Remplacement ou modification des paramètres
</div>

`ALTER SETTINGS PROFILE` offre deux façons distinctes de modifier les paramètres ainsi que les profils parents (hérités) d’un profil. Leur fonctionnement étant très différent, il est important de choisir la bonne méthode.

<div id="replacing-form">
  ### Forme de remplacement : `SETTINGS` / `INHERIT` nus
</div>

Une clause `SETTINGS` nue (sans `ADD`, `MODIFY` ni `DROP`) **remplace l’ensemble de la liste des paramètres et de tous les profils parents** du profil par exactement les éléments que vous indiquez. Tout élément précédemment présent mais non listé est supprimé sans avertissement.

```sql
CREATE SETTINGS PROFILE OR REPLACE p
    SETTINGS max_execution_time = 10, enable_lazy_columns_replication = 1;

ALTER SETTINGS PROFILE p SETTINGS max_memory_usage = 16106127360;

SHOW CREATE SETTINGS PROFILE p;
-- → CREATE SETTINGS PROFILE p SETTINGS max_memory_usage = 16106127360
-- max_execution_time and enable_lazy_columns_replication are gone.
```

:::warning
Comme la forme `SETTINGS` seule effectue un remplacement complet, l’utiliser pour « remplacer un seul paramètre » sur un profil de base déjà défini supprimera tous les autres paramètres (ainsi que tous les profils parents) de ce profil. Si vous voulez modifier un seul paramètre tout en conservant le reste, utilisez la forme incrémentielle `MODIFY`/`ADD`/`DROP` décrite ci-dessous.
:::

Il s’agit du même comportement que `SETTINGS` dans [`CREATE SETTINGS PROFILE`](../create/settings-profile.md) : la clause définit la liste complète des paramètres.

<div id="incremental-form">
  ### Forme incrémentielle : `ADD` / `MODIFY` / `DROP`
</div>

Les mots-clés `ADD`, `MODIFY` et `DROP` modifient des entrées individuelles tout en laissant le reste du profil intact :

* `ADD SETTINGS variable = value [constraints]` — ajoute un paramètre qui n&#39;est pas encore présent.
* `MODIFY SETTINGS variable = value [constraints]` — remplace l&#39;entrée correspondant à un seul paramètre. L&#39;entrée entière (valeur et contraintes) est remplacée dans son intégralité ; indiquez donc de nouveau `MIN`/`MAX`/`READONLY`/etc. si vous souhaitez les conserver.
* `DROP SETTINGS variable [,...]` — supprime les paramètres indiqués.
* `ADD PROFILES 'profile_name' [,...]` / `DROP PROFILES 'profile_name' [,...]` — ajoute ou supprime des profils parents (hérités).
* `DROP ALL SETTINGS` / `DROP ALL PROFILES` — supprime tous les paramètres ou tous les profils parents.

Plusieurs de ces clauses peuvent être combinées dans une même instruction, par exemple `DROP SETTINGS a ADD SETTINGS b = 1`.

`SET variable = value` est un alias de `MODIFY SETTINGS variable = value`. Cette forme est proposée parce que `SET` est plus naturel et que saisir la clause de remplacement `SETTINGS` alors qu&#39;une modification incrémentielle était voulue est une erreur fréquente.

<div id="examples">
  ## Exemples
</div>

Remplacez un seul paramètre tout en conservant le reste d’un profil déjà défini :

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 16106127360;
```

Ajoutez un nouveau paramètre avec contrainte et supprimez-en un autre :

```sql
ALTER SETTINGS PROFILE my_profile
    DROP SETTINGS readonly
    ADD SETTINGS max_threads = 8 MIN 4 MAX 16 WRITABLE;
```

Gérez les profils parents de façon incrémentielle :

```sql
ALTER SETTINGS PROFILE my_profile ADD PROFILES p1;
ALTER SETTINGS PROFILE my_profile DROP PROFILES p1;
```

Vérifiez toujours le résultat à l’aide de [`SHOW CREATE SETTINGS PROFILE`](../show.md) :

```sql
SHOW CREATE SETTINGS PROFILE my_profile;
```

<div id="incremental-vs-full-replacement">
  ## Mise à jour incrémentale ou remplacement complet
</div>

:::warning
Une clause `SETTINGS` utilisée seule **supprime tous les paramètres existants ainsi que tous les profils hérités (parents)** du profil avant d’appliquer les nouveaux.
:::

Pour modifier un seul paramètre tout en conservant les autres, utilisez `ADD SETTINGS` ou `MODIFY SETTINGS` (voir les exemples ci-dessous).

<div id="add-vs-modify">
  ## ADD vs MODIFY
</div>

`ADD SETTINGS` et `MODIFY SETTINGS` conservent tous deux les autres paramètres du profil, mais ils traitent différemment une entrée existante pour le *même* paramètre :

* `ADD SETTINGS variable = value ...` supprime d&#39;abord toute entrée existante pour `variable`, puis insère la nouvelle. Il **remplace donc la valeur ainsi que toutes les contraintes** de ce paramètre. Tout `MIN`, `MAX` ou attribut de modifiabilité (`READONLY`/`WRITABLE`/`CONST`/`CHANGEABLE_IN_READONLY`) précédemment défini pour `variable` et que vous ne répétez pas est supprimé.
* `MODIFY SETTINGS variable = value ...` **fusionne champ par champ** : il ne modifie que les champs que vous indiquez explicitement (la valeur, `MIN`, `MAX` ou l&#39;attribut de modifiabilité) et conserve les autres champs de ce paramètre en l&#39;état.

:::tip
En bref, utilisez `MODIFY SETTINGS` lorsque vous voulez seulement ajuster un aspect d&#39;un paramètre (par exemple uniquement la valeur, tout en conservant un `MAX` existant) ; utilisez `ADD SETTINGS` lorsque vous voulez redéfinir entièrement un paramètre.
:::

<div id="examples">
  ## Exemples
</div>

Créez un profil qui sera utilisé dans les exemples ci-dessous :

```sql
CREATE SETTINGS PROFILE OR REPLACE p SETTINGS max_execution_time = 60;
```

<div id="example-modify-settings">
  ### MODIFY SETTINGS
</div>

Ajoutez ou modifiez un seul paramètre, tout en conservant les autres :

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 20000000000;
SHOW CREATE SETTINGS PROFILE p;
-- CREATE SETTINGS PROFILE p SETTINGS
--     max_execution_time = 60,
--     max_memory_usage = 20000000000
```

Comme `MODIFY` fusionne champ par champ, le fait de ne modifier que la valeur d’un paramètre conserve les contraintes existantes :

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 20000000000 MAX 30000000000;
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 25000000000;
SHOW CREATE SETTINGS PROFILE p;
-- ... max_memory_usage = 25000000000 MAX 30000000000  -- the MAX constraint is preserved
```

<div id="example-add-settings">
  ### ADD SETTINGS
</div>

Ajoutez un paramètre (sans supprimer les autres), en le redéfinissant entièrement s’il existe déjà :

```sql
ALTER SETTINGS PROFILE p ADD SETTINGS max_threads = 8 MAX 16 READONLY;
```

Contrairement à `MODIFY`, réexécuter `ADD` avec une seule valeur supprime les contraintes précédemment définies pour ce paramètre :

```sql
ALTER SETTINGS PROFILE p ADD SETTINGS max_threads = 4;
SHOW CREATE SETTINGS PROFILE p;
-- ... max_threads = 4   -- the MAX and READONLY constraints are gone
```

<div id="example-drop-settings">
  ### DROP SETTINGS
</div>

Supprimez un ou plusieurs paramètres nommés :

```sql
ALTER SETTINGS PROFILE p DROP SETTINGS max_threads;
```

Supprimez tous les paramètres en une seule fois :

```sql
ALTER SETTINGS PROFILE p DROP ALL SETTINGS;
```

<div id="example-profiles">
  ### Travailler avec des profils hérités
</div>

Ajoutez ou supprimez des profils parents (hérités) sans affecter les paramètres propres au profil :

```sql
ALTER SETTINGS PROFILE p ADD PROFILES base_profile;
ALTER SETTINGS PROFILE p DROP PROFILES base_profile;
ALTER SETTINGS PROFILE p DROP ALL PROFILES;
```