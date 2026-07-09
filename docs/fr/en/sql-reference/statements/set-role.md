---
description: 'Documentation de l’instruction SET ROLE'
sidebar_label: 'SET ROLE'
sidebar_position: 51
slug: /sql-reference/statements/set-role
title: 'Instruction SET ROLE'
doc_type: 'reference'
---

Active les rôles de l’utilisateur courant.

```sql
SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
```

<div id="set-default-role">
  ## SET DEFAULT ROLE
</div>

Définit les rôles par défaut d’un utilisateur.

Les rôles par défaut sont automatiquement activés lors de la connexion de l’utilisateur. Vous ne pouvez définir comme rôles par défaut que des rôles qui ont déjà été accordés. Si le rôle n’est pas accordé à un utilisateur, ClickHouse lève une exception.

```sql
SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...]
```

<div id="examples">
  ## Exemples
</div>

Définir plusieurs rôles par défaut pour un utilisateur :

```sql
SET DEFAULT ROLE role1, role2, ... TO user
```

Définissez comme rôles par défaut d’un utilisateur tous les rôles qui lui ont été accordés :

```sql
SET DEFAULT ROLE ALL TO user
```

Supprimez les rôles par défaut d’un utilisateur :

```sql
SET DEFAULT ROLE NONE TO user
```

Définissez tous les rôles accordés comme rôles par défaut, à l’exception de `role1` et `role2` :

```sql
SET DEFAULT ROLE ALL EXCEPT role1, role2 TO user
```