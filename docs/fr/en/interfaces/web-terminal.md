---
description: 'Documentation du terminal web, une session `clickhouse-client` dans le navigateur via WebSocket'
sidebar_label: 'Terminal web'
sidebar_position: 22
slug: /interfaces/web-terminal
title: 'Terminal web'
doc_type: 'reference'
---

Le terminal web est une interface dans le navigateur qui propose une session `clickhouse-client` interactive via WebSocket. Il est accessible sur n’importe quel port HTTP de ClickHouse, au chemin `/webterminal`.

Accédez à `/webterminal` sur n’importe quel port HTTP de ClickHouse (par exemple, `http://localhost:8123/webterminal`) pour ouvrir le terminal.

<div id="enabling-the-feature">
  ## Activation et désactivation de la fonctionnalité
</div>

Le point de terminaison `/webterminal` est activé par défaut et est contrôlé par le paramètre serveur `enable_webterminal`. Pour le désactiver, définissez ce paramètre sur `false` ; les requêtes vers `/webterminal` renvoient alors le code d’état HTTP `403 Forbidden`.

```xml
<clickhouse>
    <enable_webterminal>false</enable_webterminal>
</clickhouse>
```

:::note
`enable_webterminal` remplace l’ancien paramètre `allow_experimental_webterminal`. L’ancien nom reste pris en charge par rétrocompatibilité si `enable_webterminal` n’est pas défini.
:::

<div id="authentication">
  ## Authentification
</div>

Le terminal web authentifie l’utilisateur en appliquant les mêmes vérifications de `Session` et de contrôle d’accès que le protocole HTTP, mais les informations d’identification sont échangées sur la connexion WebSocket établie elle-même plutôt que via la requête de mise à niveau HTTP. Une fois le handshake WebSocket terminé, le navigateur envoie le premier message au format JSON :

```json
{"type": "auth", "user": "<user>", "password": "<password>"}
```

Cela évite de placer des informations d’authentification dans les paramètres de requête d’URL ou les en-têtes `Authorization` associés à la requête d’upgrade, où elles pourraient se retrouver dans l’historique du navigateur, les journaux d’accès du serveur et les journaux du proxy inverse. Les paramètres d’URL, HTTP Basic et les en-têtes `X-ClickHouse-User`/`X-ClickHouse-Key` de la requête d’upgrade ne sont volontairement **pas** pris en compte par `/webterminal`.

Des informations d’authentification invalides amènent le serveur à fermer la connexion WebSocket avec le code `1008` ; l’interface du navigateur redemande les identifiants.

<div id="session">
  ## À quoi ressemble la session
</div>

Une fois l’utilisateur authentifié, le serveur exécute `clickhouse-client` dans un pseudo-terminal et redirige ses entrées et sorties via WebSocket. La session offre toute l’expérience `clickhouse-client`, notamment :

* La coloration syntaxique.
* L’autocomplétion.
* Les requêtes sur plusieurs lignes.
* L’historique des commandes (stocké côté serveur pendant toute la durée de la session).

Le terminal utilise [xterm.js](https://xtermjs.org/) pour l’affichage. Toutes les ressources sont servies directement depuis le binaire ClickHouse — aucun CDN tiers n’est chargé.

<div id="play-integration">
  ## Intégration avec `/play`
</div>

L&#39;UI Web SQL [`/play`](/fr/interfaces/http) intègre le terminal web sous la forme d&#39;un panneau ancrable. Affichez-le ou masquez-le à l&#39;aide de l&#39;icône du terminal dans la barre latérale, ou appuyez sur la touche `~` lorsque l&#39;éditeur de requêtes est vide. La page `/play` détecte la disponibilité de `/webterminal` lors du chargement et masque les contrôles du terminal lorsque le point de terminaison n&#39;est pas disponible (par exemple, lorsque `enable_webterminal` est défini sur `false`).

<div id="security">
  ## Considérations de sécurité
</div>

Le terminal web expose une session interactive de type shell à toute personne capable de s’authentifier sur le point de terminaison HTTP de ClickHouse ; les mêmes mises en garde que pour le protocole HTTP s’appliquent donc ici :

* Servez toujours `/webterminal` via HTTPS dans les environnements non fiables afin de protéger les identifiants et le trafic de session.
* Restreignez l’accès au niveau du réseau (pare-feu, proxy inverse ou configuration `listen_host`) de la même manière que vous restreignez l’accès au protocole HTTP.
* Le point de terminaison valide l’en-tête `Origin` par rapport à `Host` afin d’atténuer les détournements de WebSocket inter-origines ; configurez les proxies inverses en conséquence si vous terminez TLS en externe.
* Derrière un proxy inverse assurant la terminaison TLS, la connexion en amont vers ClickHouse se fait en `http` non chiffré, même si le navigateur utilise `https`, de sorte que la vérification stricte de même origine rejetterait des connexions légitimes. Pour ces déploiements, définissez `webterminal_allowed_origins` sur une liste d’origines complètes séparées par des virgules et autorisées à ouvrir des sessions WebSocket ; lorsque ce paramètre n’est pas vide, il remplace la vérification de même origine par défaut. Exemple : `<webterminal_allowed_origins>https://example.com,https://app.example.com:8443</webterminal_allowed_origins>`.

Le gestionnaire applique également la conformité au protocole WebSocket conformément à la RFC 6455 : les trames client non masquées, les opcodes réservés, les trames de contrôle surdimensionnées ou fragmentées, ainsi que les bits RSV réservés sont rejetés avec des codes de fermeture signalant une erreur de protocole.

<div id="platform">
  ## Disponibilité de la plateforme
</div>

Le gestionnaire est compilé sur toutes les plateformes prises en charge par ClickHouse. La couche de pseudoterminal utilisée par le lanceur `clickhouse-client` intégré repose sur des primitives POSIX portables (`posix_openpt`/`grantpt`/`unlockpt`), avec une branche de code spécifique à Linux qui utilise `ptsname_r`, compatible avec un usage multithread. Les liens vers `/webterminal` sur la page de démarrage de ClickHouse et dans `/play` sont automatiquement masqués lorsque le point de terminaison n’est pas disponible (par exemple, lorsque `enable_webterminal` est défini sur `false`).