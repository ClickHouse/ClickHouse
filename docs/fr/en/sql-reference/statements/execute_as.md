---
description: "Documentation de l’instruction EXECUTE AS"
sidebar_label: 'EXECUTE AS'
sidebar_position: 53
slug: /sql-reference/statements/execute_as
title: "Instruction EXECUTE AS"
doc_type: 'référence'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

<div id="execute-as-statement">
  # Instruction EXECUTE AS
</div>

Permet d’exécuter des requêtes au nom d’un autre utilisateur.

<div id="syntax">
  ## Syntaxe
</div>

```sql
EXECUTE AS target_user;
EXECUTE AS target_user subquery;
```

La première forme (sans `subquery`) fait en sorte que toutes les requêtes suivantes de la session en cours soient exécutées pour le compte du `target_user` spécifié.

La deuxième forme (avec `subquery`) exécute uniquement la `subquery` spécifiée pour le compte du `target_user` spécifié.

Pour fonctionner, les deux formes nécessitent que le paramètre de configuration `access_control_improvements.allow_impersonate_user`
soit défini sur `1` et que le privilège `IMPERSONATE` soit accordé. Par exemple, les commandes suivantes

```sql
GRANT IMPERSONATE ON user1 TO user2;
GRANT IMPERSONATE ON * TO user3;
```

autoriser l’utilisateur `user2` à exécuter des commandes `EXECUTE AS user1 ...` et autoriser également l’utilisateur `user3` à exécuter des commandes en tant que n’importe quel utilisateur.

Lorsqu’on se fait passer pour un autre utilisateur, la fonction [currentUser()](/fr/sql-reference/functions/other-functions#currentUser) renvoie le nom de cet autre utilisateur,
et la fonction [authenticatedUser()](/fr/sql-reference/functions/other-functions#authenticatedUser) renvoie le nom de l’utilisateur qui a réellement été authentifié.

<div id="examples">
  ## Exemples
</div>

```sql
SELECT currentUser(), authenticatedUser(); -- outputs "default    default"
CREATE USER james;
EXECUTE AS james SELECT currentUser(), authenticatedUser(); -- outputs "james    default"
```