---
description: 'Permet d’accéder au système de fichiers pour lister les fichiers et renvoyer leurs métadonnées ainsi que leur contenu.'
sidebar_label: 'filesystem'
sidebar_position: 62
slug: /sql-reference/table-functions/filesystem
title: 'filesystem'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="filesystem-table-function">
  # Fonction de table filesystem
</div>

<CloudNotSupportedBadge />

Parcourt récursivement un répertoire et renvoie une table contenant les métadonnées des fichiers (chemins, tailles, types, droits d’accès, dates de modification) et, éventuellement, leur contenu.

En mode `clickhouse-server`, le chemin doit se trouver dans le répertoire [user&#95;files&#95;path](/fr/operations/server-configuration-parameters/settings.md#user_files_path). Les liens symboliques situés dans `user_files_path` et pointant à l’extérieur de celui-ci sont suivis, mais seules les entrées dont le chemin (via le lien symbolique) commence par `user_files_path` sont renvoyées.

En mode `clickhouse-local`, il n’y a aucune restriction sur les chemins.

<div id="syntax">
  ## Syntaxe
</div>

```sql
filesystem([path])
```

<div id="arguments">
  ## Arguments
</div>

| Paramètre | Description                                                                                                                                                                                                                        |
| --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `path`    | Le répertoire à lister. Il peut s’agir d’un chemin absolu (il doit se trouver dans `user_files_path` en mode serveur) ou d’un chemin relatif à `user_files_path`. S’il est vide ou omis, `user_files_path` est utilisé par défaut. |

<div id="returned_columns">
  ## Colonnes renvoyées
</div>

| Colonne             | Type                       | Description                                                                                                                                                                                                                                                                                 |
| ------------------- | -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `path`              | `String`                   | Répertoire contenant l’entrée (sans inclure le nom du fichier ou du répertoire lui-même).                                                                                                                                                                                                   |
| `name`              | `String`                   | Nom du fichier ou du répertoire (le dernier composant du chemin).                                                                                                                                                                                                                           |
| `file`              | `String` (ALIAS of `name`) | alias de la colonne `name`.                                                                                                                                                                                                                                                                 |
| `type`              | `Enum8`                    | Type de fichier : `'none'`, `'not_found'`, `'regular'`, `'directory'`, `'symlink'`, `'block'`, `'character'`, `'fifo'`, `'socket'`, `'unknown'`.                                                                                                                                            |
| `size`              | `Nullable(UInt64)`         | Taille du fichier en octets (pour les fichiers ordinaires). `NULL` pour les fichiers non ordinaires (répertoires, liens symboliques, etc.) et en cas d’erreur.                                                                                                                              |
| `depth`             | `UInt16`                   | Profondeur de récursion. `0` pour le répertoire interrogé lui-même et ses enfants directs, `1` pour les entrées situées un niveau plus bas, et ainsi de suite.                                                                                                                              |
| `modification_time` | `Nullable(DateTime64(6))`  | Heure de la dernière modification, avec une précision à la microseconde. `NULL` en cas d’erreur.                                                                                                                                                                                            |
| `is_symlink`        | `Bool`                     | Indique si l’entrée est un lien symbolique.                                                                                                                                                                                                                                                 |
| `content`           | `Nullable(String)`         | Contenu du fichier (pour les fichiers ordinaires). `NULL` pour les fichiers non ordinaires (répertoires, liens symboliques, etc.). Les erreurs de lecture lèvent une exception. La lecture de cette colonne déclenche de vraies E/S fichier ; omettez-la donc si vous n’en avez pas besoin. |
| `owner_read`        | `Bool`                     | Le propriétaire dispose de l’autorisation de lecture.                                                                                                                                                                                                                                       |
| `owner_write`       | `Bool`                     | Le propriétaire dispose de l’autorisation d’écriture.                                                                                                                                                                                                                                       |
| `owner_exec`        | `Bool`                     | Le propriétaire dispose de l’autorisation d’exécution.                                                                                                                                                                                                                                      |
| `group_read`        | `Bool`                     | Le groupe dispose de l’autorisation de lecture.                                                                                                                                                                                                                                             |
| `group_write`       | `Bool`                     | Le groupe dispose de l’autorisation d’écriture.                                                                                                                                                                                                                                             |
| `group_exec`        | `Bool`                     | Le groupe dispose de l’autorisation d’exécution.                                                                                                                                                                                                                                            |
| `others_read`       | `Bool`                     | Les autres disposent de l’autorisation de lecture.                                                                                                                                                                                                                                          |
| `others_write`      | `Bool`                     | Les autres disposent de l’autorisation d’écriture.                                                                                                                                                                                                                                          |
| `others_exec`       | `Bool`                     | Les autres disposent de l’autorisation d’exécution.                                                                                                                                                                                                                                         |
| `set_gid`           | `Bool`                     | Bit Set-GID.                                                                                                                                                                                                                                                                                |
| `set_uid`           | `Bool`                     | Bit Set-UID.                                                                                                                                                                                                                                                                                |
| `sticky_bit`        | `Bool`                     | Sticky bit.                                                                                                                                                                                                                                                                                 |

Seules les colonnes effectivement utilisées dans la query sont calculées. Sélectionner un sous-ensemble de colonnes (en omettant notamment `content`) est donc efficace.

<div id="examples">
  ## Exemples
</div>

<div id="list-files">
  ### Lister les fichiers dans user_files
</div>

```sql
SELECT name, type, size, depth
FROM filesystem()
ORDER BY name;
```

<div id="find-large-files">
  ### Repérer les fichiers volumineux
</div>

```sql
SELECT path, name, size
FROM filesystem()
WHERE type = 'regular' AND size > 1000000
ORDER BY size DESC;
```

<div id="read-contents">
  ### Lire le contenu du fichier
</div>

```sql
SELECT name, content
FROM filesystem('my_directory')
WHERE name LIKE '%.csv';
```

<div id="list-immediate">
  ### Lister uniquement les enfants directs
</div>

```sql
SELECT name, type
FROM filesystem('my_directory')
WHERE depth = 0;
```