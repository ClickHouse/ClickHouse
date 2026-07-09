---
description: 'Guide de l’authentification par JWT et des utilisateurs éphémères dans ClickHouse Cloud'
sidebar_label: 'JWT'
sidebar_position: 55
slug: /operations/external-authenticators/jwt
title: 'Authentification par JWT'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

ClickHouse peut authentifier les utilisateurs à l’aide de JSON Web Tokens (JWT). Contrairement à d’autres mécanismes d’authentification externes comme [LDAP](/fr/operations/external-authenticators/ldap) ou [Kerberos](/fr/operations/external-authenticators/kerberos), l’authentification JWT ne vérifie pas l’identité d’utilisateurs existants. À la place, elle crée dynamiquement des **utilisateurs éphémères** à partir des claims incluses dans chaque jeton. Ces utilisateurs n’existent qu’en mémoire, reçoivent des droits d’accès dérivés des claims du jeton et sont automatiquement supprimés à l’expiration du jeton.

Cela rend l’authentification JWT fondamentalement différente des méthodes basées sur un mot de passe ou un certificat : il n’existe pas d’instruction `CREATE USER ... IDENTIFIED WITH jwt`, et toute tentative en ce sens déclenche une exception. Les utilisateurs JWT sont entièrement gérés par le cycle de vie du jeton.

<div id="overview">
  ## Vue d’ensemble
</div>

Le flux d’authentification fonctionne comme suit :

1. Un client présente un JWT signé via l’un des mécanismes de transport pris en charge (en-tête HTTP `Authorization: Bearer`, protocole natif TCP ou champ gRPC `jwt`).
2. ClickHouse valide la signature du jeton.
3. Les claims requis (`exp`, `iat`, `iss`, `sub`, `aud`) sont vérifiés.
4. Un utilisateur éphémère est créé en mémoire avec des droits d’accès dérivés des claims de jeton `clickhouse:grants` et `clickhouse:roles`, puis croisés avec une limite d’autorisation.
5. Lorsque le jeton expire, une tâche de garbage collection en arrière-plan supprime l’utilisateur.

<div id="token-claims">
  ## Claims du jeton
</div>

<div id="required-claims">
  ### Claims obligatoires
</div>

Chaque JWT présenté à ClickHouse doit contenir les claims suivants :

| Claim | Description                                                                                               |
| ----- | --------------------------------------------------------------------------------------------------------- |
| `alg` | Algorithme de signature (claim d&#39;en-tête). Valeurs prises en charge : `HS256`, `RS256`, `ES256`.      |
| `exp` | Heure d&#39;expiration. Définit le `valid_until` de l&#39;utilisateur éphémère.                           |
| `iat` | Heure d&#39;émission. Utilisée pour empêcher la réutilisation d&#39;anciens tokens pour la même identité. |
| `iss` | Émetteur. Comparé à l&#39;émetteur attendu du fournisseur.                                                |
| `sub` | Sujet. Devient une partie du nom d’utilisateur généré.                                                    |
| `aud` | Audience. Comparée à l&#39;audience attendue du fournisseur.                                              |

Le claim d&#39;en-tête `kid` (ID de clé) est également requis lorsque la résolution de clés basée sur JWKS est utilisée.

:::note Le mode JWKS ne prend en charge que les clés RSA
Alors que les fournisseurs à clé statique acceptent `HS256`, `RS256` ou `ES256`, les fournisseurs basés sur JWKS n&#39;acceptent que les JWK dont le `kty` est `RSA` (c.-à-d. les tokens signés avec `RS256`). Les tokens signés avec des clés HMAC (`HS256`) ou EC (`ES256`) ne peuvent pas être vérifiés via un endpoint JWKS et seront rejetés.
:::

<div id="other-recognized-claims">
  ### Autres claims reconnues
</div>

| Claim | Description                                                                                                                    |
| ----- | ------------------------------------------------------------------------------------------------------------------------------ |
| `nbf` | Heure « not-before ». Cette claim n’est pas obligatoire, mais si elle est présente, les tokens sont rejetés avant cette heure. |
| `jti` | Réservé. Accepté dans les tokens, mais actuellement ni validé ni utilisé.                                                      |

<div id="optional-claims">
  ### Claims facultatives
</div>

| Claim                                                                                                                                                                      | Nom par défaut      | Description                                                                                                                                                         |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Grants                                                                                                                                                                     | `clickhouse:grants` | Un tableau JSON de fragments SQL `GRANT`, par ex. `["SELECT ON db.*", "INSERT ON db.table1"]`. Chaque élément est analysé comme le corps d’une instruction `GRANT`. |
| Roles                                                                                                                                                                      | `clickhouse:roles`  | Un tableau JSON de noms de rôles à attribuer, par ex. `["analyst", "reader"]`.                                                                                      |
| Les noms de claims par défaut peuvent être redéfinis avec des noms de claims personnalisés si votre fournisseur d’identité utilise des conventions de nommage différentes. |                     |                                                                                                                                                                     |

<div id="example-token-header-and-payload">
  ### Exemple d’en-tête et de charge utile du jeton
</div>

```json
{
  "alg": "RS256",
  "kid": "my-key-id"
}
```

```json
{
  "iss": "https://idp.example.com",
  "sub": "jane.doe",
  "aud": "my-clickhouse-cluster",
  "exp": 1719504000,
  "iat": 1719500400,
  "clickhouse:grants": ["SELECT ON analytics.*", "INSERT ON analytics.events"],
  "clickhouse:roles": ["analyst"]
}
```

<div id="ephemeral-user-behavior">
  ## Comportement des utilisateurs éphémères
</div>

Les utilisateurs JWT se distinguent des utilisateurs ClickHouse classiques sur plusieurs points importants.

<div id="identity-and-naming">
  ### Identité et dénomination
</div>

Chaque utilisateur JWT reçoit un UUID déterministe calculé à partir des claims `iss`, `sub` et `aud`. Cet UUID est **stable** d’une connexion à l’autre. Un utilisateur qui se connecte plusieurs fois avec des jetons différents (mais le même émetteur, le même subject et la même audience) obtient toujours le même UUID.

Le nom d’utilisateur, en revanche, est **volatile**. Il est construit comme suit :

```text
JWT::<issuer>::<audience>::<subject>::<claims_hash>
```

La partie `<claims_hash>` change chaque fois que les claims `clickhouse:roles` ou `clickhouse:grants` sont modifiés. Cela signifie que des tokens avec des ensembles de rôles ou de grants différents produisent des noms d&#39;utilisateur différents, même pour une même identité.

<div id="access-rights">
  ### Droits d’accès
</div>

Les droits d’accès effectifs sont calculés ainsi :

```text
effective_rights = permission_limit ∩ (token_grants ∪ token_roles)
```

où `permission_limit` désigne l’ensemble des droits d’accès détenus par un rôle ou un utilisateur de référence défini comme limite supérieure. Les droits demandés par le token qui dépassent cette limite sont ignorés silencieusement.

<div id="token-freshness">
  ### Fraîcheur des jetons
</div>

ClickHouse suit la claim `iat` (date d’émission) du jeton authentifié le plus récemment pour chaque identité stable. Si un jeton avec une valeur `iat` égale ou antérieure à la valeur enregistrée est présenté, le serveur réutilise l’utilisateur éphémère existant sans réévaluer les claims. Cela empêche des jetons plus anciens de réduire les permissions d’un utilisateur.

<div id="lifetime-and-garbage-collection">
  ### Durée de vie et garbage collection
</div>

Les utilisateurs éphémères sont créés lorsqu’un jeton est authentifié pour la première fois, puis supprimés par une tâche de garbage collection en arrière-plan après l’expiration de `valid_until` (dérivé de `exp`). L’intervalle du GC est contrôlé par le paramètre `gc_interval` (par défaut : 5 minutes).

Entre deux exécutions du GC, les utilisateurs expirés peuvent encore apparaître dans `system.users`, mais ils ne peuvent plus s’authentifier.

<div id="persistent-access-assignments">
  ### Attributions d’accès persistantes
</div>

Comme l’UUID est stable, vous pouvez attribuer des profils de paramètres, des quotas, des politiques de ligne et des politiques de masquage de colonnes à un utilisateur JWT à l’aide d’instructions SQL. Ces attributions persistent dans le stockage de contrôle d’accès (sur disque ou dans ZooKeeper) et restent valides après l’expiration du jeton et une nouvelle authentification.

Faites référence à l’utilisateur par son nom d’utilisateur actuel :

```sql
ALTER SETTINGS PROFILE my_profile ADD TO 'JWT::ClickHouse::my-service-id::jane.doe::<claims-hash>';
```

:::note
Le nom d’utilisateur et l’UUID d’une identité donnée figurent dans les colonnes `name` et `id` de `system.users` tant que l’utilisateur est actif.
:::

Notez que `ALTER USER` ne fonctionne pas directement avec les utilisateurs JWT, car ils sont en lecture seule. Pour attribuer des profils de paramètres, des quotas ou des politiques, utilisez les instructions `ALTER SETTINGS PROFILE`, `ALTER QUOTA` ou `ALTER ROW POLICY`, comme indiqué ci-dessus.

<div id="differences-from-regular-users">
  ## Différences avec les utilisateurs standard
</div>

| Fonctionnalité                        | Utilisateurs JWT                                                         | Utilisateurs standard                                            |
| ------------------------------------- | ------------------------------------------------------------------------ | ---------------------------------------------------------------- |
| Création                              | Automatique à partir des claims du jeton                                 | Instruction `CREATE USER`                                        |
| Stockage                              | En mémoire uniquement (éphémère)                                         | Disque, ZooKeeper ou fichier de configuration                    |
| `CREATE USER ... IDENTIFIED WITH jwt` | Non pris en charge (lève une exception)                                  | Tous les autres types d&#39;authentification sont pris en charge |
| `ALTER USER` / `DROP USER`            | Non pris en charge                                                       | Pris en charge                                                   |
| Sauvegarde et restauration            | Non incluses                                                             | Incluses                                                         |
| Nom d&#39;utilisateur                 | Généré automatiquement, volatile                                         | Choisi par l&#39;administrateur, fixe                            |
| UUID                                  | Déterministe à partir de `iss`+`sub`+`aud`                               | Aléatoire au moment de la création                               |
| Durée de vie                          | Limitée par `exp` du jeton                                               | Jusqu&#39;à suppression explicite                                |
| Droits d&#39;accès                    | Dérivés des claims du jeton, plafonnés par la limite d&#39;autorisations | Accordés explicitement via `GRANT`                               |
| Restrictions d&#39;hôte               | Configuration réseau par fournisseur                                     | Clause `HOST` par utilisateur                                    |
| Profils de paramètres                 | Assignables par UUID (persistants)                                       | Directement configurables                                        |
| Quotas et politiques de ligne         | Assignables par UUID (persistants)                                       | Directement configurables                                        |
| Rôles par défaut                      | Non configurables                                                        | Configurables                                                    |

<div id="sql-security-definer-views">
  ## Vues SQL SECURITY DEFINER
</div>

Lorsqu&#39;un utilisateur JWT éphémère crée une vue avec `SQL SECURITY DEFINER`, le serveur crée automatiquement une copie fantôme persistante de l&#39;utilisateur pour faire office de définisseur de la vue. Cet utilisateur fantôme :

* A pour nom `<original_jwt_username>:definer`
* Utilise `NO_AUTHENTICATION` (il ne peut pas être utilisé pour se connecter)
* Conserve les mêmes droits d&#39;accès que l&#39;utilisateur JWT d&#39;origine au moment de la création de la vue

Cela garantit que la vue continue de fonctionner après l&#39;expiration du jeton de l&#39;utilisateur éphémère et la suppression de l&#39;utilisateur d&#39;origine par le garbage collector.

<div id="client-usage">
  ## Utilisation du client
</div>

<div id="passing-token-directly">
  ### Transmettre un jeton directement
</div>

Utilisez l’option `--jwt` avec `clickhouse-client` pour vous authentifier à l’aide d’un jeton obtenu au préalable :

```bash
clickhouse-client --host your-instance.clickhouse.cloud --secure --jwt '<your_jwt_token>'
```

:::note
L’option `--jwt` est incompatible avec `--user`. Lorsque `--jwt` est indiqué, le nom d’utilisateur est déduit du jeton.
:::

<div id="http-interface">
  ### Interface HTTP
</div>

Envoyez le jeton sous forme de Bearer token dans l’en-tête `Authorization` :

```bash
curl -H 'Authorization: Bearer <your_jwt_token>' \
    'https://your-instance.clickhouse.cloud:8443/?query=SELECT+currentUser()'
```

:::warning
Envoyez toujours les JWT via HTTPS. Un Bearer token envoyé en HTTP non chiffré est exposé à toute personne se trouvant sur le trajet réseau et équivaut à divulguer l’identifiant d’authentification.
:::

<div id="oauth2-device-code-login">
  ### Connexion OAuth2 par code d’appareil
</div>

Le `clickhouse-client` prend en charge un flux interactif OAuth2 par code d’appareil via l’option `--login`. Pour les endpoints ClickHouse Cloud, le client effectue automatiquement un échange de tokens afin d’obtenir un JWT spécifique à ClickHouse. Les tokens sont actualisés de manière transparente pendant la session. Lorsqu’un nouveau token est obtenu, le client se reconnecte automatiquement.

```bash
clickhouse-client --host your-instance.clickhouse.cloud --login
```

<div id="clickhouse-cloud-built-in">
  ## Authentificateur JWT intégré à ClickHouse Cloud
</div>

Chaque service ClickHouse Cloud inclut un authentificateur JWT prédéfini, utilisé par SQL Console et par le flux `--login` de `clickhouse-client`. Cet authentificateur est configuré avec :

| Paramètre        | Valeur                                                     |
| ---------------- | ---------------------------------------------------------- |
| `iss` (émetteur) | `ClickHouse`                                               |
| `aud` (audience) | L’UUID du service (visible dans l’URL de la Cloud Console) |
| `sub` (sujet)    | L’adresse e-mail de votre compte ClickHouse Cloud          |

L’authentificateur intégré a une limite d’autorisations définie sur le rôle `default_role` et l’utilisateur `default`. Cela signifie que les droits effectifs de tout utilisateur JWT correspondent à l’intersection des grants détenus par ces deux entités : un jeton ne peut donc jamais obtenir plus de privilèges que ce que `default_role` et `default` sont autorisés à faire.

Vous n’avez rien à configurer pour utiliser cet authentificateur. Il est créé automatiquement lors de la création du service.

<div id="interserver-communication">
  ## Communication interserveur
</div>

Lorsqu’une requête est acheminée vers un autre segment ou une autre réplique, le jeton JWT est inclus dans le protocole interserveur. Le nœud distant authentifie de nouveau le jeton de manière indépendante, en créant son propre utilisateur éphémère.

<div id="troubleshooting">
  ## Dépannage
</div>

* **Aucun droit d’accès accordé :** Le rôle ou l’utilisateur mentionné ne dispose peut-être pas des grants requis. Assurez-vous que les rôles référencés dans `clickhouse:roles` existent et qu’ils incluent les grants appropriés.
* **Jeton rejeté :** Vérifiez que `iss`, `aud` et l’algorithme de signature de votre jeton correspondent bien à ce qu’attend le fournisseur JWT. Si JWKS est utilisé, assurez-vous que le `kid` du jeton correspond à une clé du jeu de clés du fournisseur.
* **L’utilisateur disparaît entre les requêtes :** Les utilisateurs éphémères sont supprimés à l’expiration du jeton. Utilisez un client qui prend en charge le renouvellement du jeton (par exemple, le mode `--login`) pour les sessions de longue durée.
* **`CREATE USER ... IDENTIFIED WITH jwt` échoue :** C’est normal. Les utilisateurs JWT ne peuvent pas être créés via DDL. Ils sont entièrement gérés par le cycle de vie du jeton.