---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 48
toc_title: "Le Contr\xF4le d'acc\xE8s et de Gestion de Compte"
---

# Le Contrôle d'accès et de Gestion de Compte {#access-control}

Clickhouse prend en charge la gestion du contrôle d'accès basée sur [RBAC](https://en.wikipedia.org/wiki/Role-based_access_control) approche.

Entités d'accès ClickHouse:
- [Compte d'utilisateur](#user-account-management)
- [Rôle](#role-management)
- [La Ligne Politique](#row-policy-management)
- [Les Paramètres De Profil](#settings-profiles-management)
- [Quota](#quotas-management)

Vous pouvez configurer des entités d'accès à l'aide de:

-   Flux de travail piloté par SQL.

    Vous avez besoin de [permettre](#enabling-access-control) cette fonctionnalité.

-   Serveur [les fichiers de configuration](configuration-files.md) `users.xml` et `config.xml`.

Nous vous recommandons D'utiliser un workflow piloté par SQL. Les deux méthodes de configuration fonctionnent simultanément, donc si vous utilisez les fichiers de configuration du serveur pour gérer les comptes et les droits d'accès, vous pouvez passer doucement au flux de travail piloté par SQL.

!!! note "Avertissement"
    Vous ne pouvez pas gérer la même entité d'accès par les deux méthodes de configuration simultanément.

## Utilisation {#access-control-usage}

Par défaut, le serveur ClickHouse fournit le compte utilisateur `default` ce qui n'est pas autorisé à utiliser le contrôle D'accès piloté par SQL et la gestion de compte, mais a tous les droits et autorisations. Le `default` compte d'utilisateur est utilisé dans tous les cas, lorsque l'utilisateur n'est pas défini, par exemple, lors de la connexion du client ou dans les requêtes distribuées. Dans le traitement des requêtes distribuées, un compte utilisateur par défaut est utilisé si la configuration du serveur ou du cluster ne spécifie pas [d'utilisateur et mot de passe](../engines/table-engines/special/distributed.md) propriété.

Si vous commencez simplement à utiliser ClickHouse, vous pouvez utiliser le scénario suivant:

1.  [Permettre](#enabling-access-control) Contrôle D'accès piloté par SQL et gestion de compte pour le `default` utilisateur.
2.  Connexion en vertu de la `default` compte d'utilisateur et de créer tous les utilisateurs. N'oubliez pas de créer un compte d'administrateur (`GRANT ALL ON *.* WITH GRANT OPTION TO admin_user_account`).
3.  [Restreindre les autorisations](settings/permissions-for-queries.md#permissions_for_queries) pour l' `default` utilisateur et désactiver le contrôle D'accès piloté par SQL et la gestion des comptes pour elle.

### Propriétés de la Solution actuelle {#access-control-properties}

-   Vous pouvez accorder des autorisations pour les bases de données et les tables même si elles n'existent pas.
-   Si une table a été supprimée, tous les privilèges correspondant à cette table ne sont pas révoqués. Ainsi, si une nouvelle table est créée plus tard avec le même nom, tous les privilèges redeviennent réels. Pour révoquer les privilèges correspondant à la table supprimée, vous devez effectuer, par exemple, l' `REVOKE ALL PRIVILEGES ON db.table FROM ALL` requête.
-   Il n'y a pas de paramètres de durée de vie pour les privilèges.

## Compte d'utilisateur {#user-account-management}

Un compte d'utilisateur est une entité qui permet d'autoriser quelqu'un à ClickHouse. Un compte utilisateur contient:

-   Informations d'Identification.
-   [Privilège](../sql-reference/statements/grant.md#grant-privileges) qui définissent l'étendue des requêtes que l'utilisateur peut effectuer.
-   Hôtes à partir desquels la connexion au serveur ClickHouse est autorisée.
-   Rôles accordés et par défaut.
-   Paramètres avec leurs contraintes qui s'appliquent par défaut lors de la connexion de l'utilisateur.
-   Profils de paramètres assignés.

Des privilèges à un compte d'utilisateur peuvent être accordés par [GRANT](../sql-reference/statements/grant.md) requête ou en attribuant [rôle](#role-management). Pour révoquer les privilèges d'un utilisateur, ClickHouse fournit [REVOKE](../sql-reference/statements/revoke.md) requête. Pour lister les privilèges d'un utilisateur, utilisez - [SHOW GRANTS](../sql-reference/statements/show.md#show-grants-statement) déclaration.

Gestion des requêtes:

-   [CREATE USER](../sql-reference/statements/create.md#create-user-statement)
-   [ALTER USER](../sql-reference/statements/alter.md#alter-user-statement)
-   [DROP USER](../sql-reference/statements/misc.md#drop-user-statement)
-   [SHOW CREATE USER](../sql-reference/statements/show.md#show-create-user-statement)

### Paramètres Application {#access-control-settings-applying}

Les paramètres peuvent être définis de différentes manières: pour un compte utilisateur, dans ses profils de rôles et de paramètres accordés. Lors d'une connexion utilisateur, si un paramètre est défini dans différentes entités d'accès, la valeur et les contraintes de ce paramètre sont appliquées par les priorités suivantes (de plus haut à plus bas):

1.  Paramètre de compte utilisateur.
2.  Les paramètres de rôles par défaut du compte d'utilisateur. Si un paramètre est défini dans certains rôles, l'ordre de la mise en application n'est pas défini.
3.  Les paramètres dans les profils de paramètres attribués à un utilisateur ou à ses rôles par défaut. Si un paramètre est défini dans certains profils, l'ordre d'application des paramètres n'est pas défini.
4.  Paramètres appliqués à l'ensemble du serveur par défaut [profil par défaut](server-configuration-parameters/settings.md#default-profile).

## Rôle {#role-management}

Le rôle est un conteneur pour l'accès des entités qui peuvent être accordées à un compte d'utilisateur.

Rôle contient:

-   [Privilège](../sql-reference/statements/grant.md#grant-privileges)
-   Paramètres et contraintes
-   Liste des rôles attribués

Gestion des requêtes:

-   [CREATE ROLE](../sql-reference/statements/create.md#create-role-statement)
-   [ALTER ROLE](../sql-reference/statements/alter.md#alter-role-statement)
-   [DROP ROLE](../sql-reference/statements/misc.md#drop-role-statement)
-   [SET ROLE](../sql-reference/statements/misc.md#set-role-statement)
-   [SET DEFAULT ROLE](../sql-reference/statements/misc.md#set-default-role-statement)
-   [SHOW CREATE ROLE](../sql-reference/statements/show.md#show-create-role-statement)

Les privilèges d'un rôle peuvent être accordés par [GRANT](../sql-reference/statements/grant.md) requête. Pour révoquer les privilèges D'un rôle ClickHouse fournit [REVOKE](../sql-reference/statements/revoke.md) requête.

## La Ligne Politique {#row-policy-management}

La stratégie de ligne est un filtre qui définit les lignes disponibles pour un utilisateur ou pour un rôle. La stratégie de ligne contient des filtres pour une table spécifique et une liste de rôles et / ou d'utilisateurs qui doivent utiliser cette stratégie de ligne.

Gestion des requêtes:

-   [CREATE ROW POLICY](../sql-reference/statements/create.md#create-row-policy-statement)
-   [ALTER ROW POLICY](../sql-reference/statements/alter.md#alter-row-policy-statement)
-   [DROP ROW POLICY](../sql-reference/statements/misc.md#drop-row-policy-statement)
-   [SHOW CREATE ROW POLICY](../sql-reference/statements/show.md#show-create-row-policy-statement)

## Les Paramètres De Profil {#settings-profiles-management}

Paramètres profil est une collection de [paramètre](settings/index.md). Le profil paramètres contient les paramètres et les contraintes, ainsi que la liste des rôles et/ou des utilisateurs auxquels ce quota est appliqué.

Gestion des requêtes:

-   [CREATE SETTINGS PROFILE](../sql-reference/statements/create.md#create-settings-profile-statement)
-   [ALTER SETTINGS PROFILE](../sql-reference/statements/alter.md#alter-settings-profile-statement)
-   [DROP SETTINGS PROFILE](../sql-reference/statements/misc.md#drop-settings-profile-statement)
-   [SHOW CREATE SETTINGS PROFILE](../sql-reference/statements/show.md#show-create-settings-profile-statement)

## Quota {#quotas-management}

Le Quota limite l'utilisation des ressources. Voir [Quota](quotas.md).

Quota contient un ensemble de limites pour certaines durées, et la liste des rôles et/ou des utilisateurs qui devrait utiliser ce quota.

Gestion des requêtes:

-   [CREATE QUOTA](../sql-reference/statements/create.md#create-quota-statement)
-   [ALTER QUOTA](../sql-reference/statements/alter.md#alter-quota-statement)
-   [DROP QUOTA](../sql-reference/statements/misc.md#drop-quota-statement)
-   [SHOW CREATE QUOTA](../sql-reference/statements/show.md#show-create-quota-statement)

## Activation du contrôle D'accès piloté par SQL et de la gestion de Compte {#enabling-access-control}

-   Configurez un répertoire pour le stockage des configurations.

    Clickhouse stocke les configurations d'entité d'accès dans le dossier défini dans [access_control_path](server-configuration-parameters/settings.md#access_control_path) paramètre de configuration du serveur.

-   Activez le contrôle D'accès piloté par SQL et la gestion de compte pour au moins un compte d'utilisateur.

    Par défaut, le contrôle D'accès piloté par SQL et la gestion des comptes sont activés pour tous les utilisateurs. Vous devez configurer au moins un utilisateur dans le `users.xml` fichier de configuration et affecter 1 au [access_management](settings/settings-users.md#access_management-user-setting) paramètre.

[Article Original](https://clickhouse.tech/docs/en/operations/access_rights/) <!--hide-->
