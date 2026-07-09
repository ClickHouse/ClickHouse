---
description: 'Présentation de la politique de backport de ClickHouse et de l’automatisation associée'
sidebar_label: 'Système de backport'
sidebar_position: 56
slug: /development/backports
title: 'Système de backport'
doc_type: 'reference'
---

Ce document décrit la politique de backport de ClickHouse et le système automatisé qui l’applique.

<div id="release-model">
  ## Modèle de publication
</div>

Les versions de ClickHouse suivent le schéma `YY.M.patch.build-type`, où `YY` représente l’année sur deux chiffres, `M` le mois de la release (sans zéro initial), `patch` le numéro de patch au sein de la branche, `build` un numéro de build croissant, et `type` vaut `stable` ou `lts`.

Exemple : `25.3.8.23-lts` — LTS de mars 2025, patch 8, build 23.

Il existe deux canaux de release :

* Les releases **stable** sont publiées environ une fois par mois. Les trois releases stable les plus récentes reçoivent des patchs, ce qui représente environ trois mois de support actif par release.
* Les releases **LTS (Long-Term Support)** sont publiées en mars et en août chaque année. Deux versions LTS sont prises en charge simultanément, chacune pendant au moins 12 mois.

Il est recommandé aux utilisateurs exécutant des charges de travail en production d’utiliser soit la dernière release stable, soit une release LTS, et de passer rapidement aux nouvelles versions de patch, car les releases de patch n’introduisent pas de breaking changes.

<div id="backport-policy">
  ## Politique de backport
</div>

Tous les changements ne font pas l’objet d’un backport. L’objectif est de maintenir la stabilité des branches de release ; le périmètre des backports est donc volontairement restreint :

* **Correctifs de sécurité** — toujours rétroportés.
* **Correctifs de bogues critiques** (exceptions (`logical errors`), perte de données, résultats erronés, problèmes de RBAC) — sélectionnés automatiquement pour le backport selon les règles générales de backport ; identifiés par le label `pr-critical-bugfix`, qui entraîne l’ajout automatique de `pr-must-backport`.
* **Correctifs de stabilité et de régression** — rétroportés lorsque le risque du changement est faible par rapport au risque de laisser le bogue en place ; identifiés par `pr-must-backport`, ajouté manuellement par les mainteneurs.
* **Correctifs de bogues mineurs avec une solution de contournement disponible** — généralement non rétroportés afin d’éviter de déstabiliser les branches de release.
* **Nouvelles fonctionnalités, améliorations, optimisations des performances** — non rétroportées.

Le label `pr-must-backport` est le mécanisme de dérogation manuel utilisé par les mainteneurs pour marquer une PR en vue d’un backport. Le label `pr-critical-bugfix` entraîne l’ajout automatique de `pr-must-backport` par le hook d’intégration continue (voir `pr_labels_and_category.py`).

**Escalade des conflits.** Lorsque le backport automatique ne permet pas de résoudre les conflits de merge, une cherry-pick PR doit malgré tout être créée et attribuée à l’auteur, à la personne ayant effectué le merge ainsi qu’aux assignees existants de l’original PR, afin qu’une personne puisse résoudre les conflits et finaliser le backport.

<div id="backport-tool">
  ## Outil de backport
</div>

La politique de backport décrite ci-dessus est mise en œuvre par l’outil automatisé situé dans `tests/ci/cherry_pick.py`. Cet outil s’exécute sous forme de workflow GitHub Actions sur l’infrastructure ClickHouse et couvre l’ensemble des besoins : découverte des branches de release actives, sélection des PR éligibles au backport, exécution de la procédure de cherry-pick et de backport en deux étapes, gestion des conflits, application de la politique de délai et synchronisation des labels.

L’objectif à long terme est d’extraire cette implémentation dans un outil Python autonome open-source que d’autres projets pourront adopter. La conception cible est la suivante :

* **Configurable** — tous les paramètres de la politique (labels de qualification, fenêtre de délai, seuils de PR stale, comportement lors du rolling-out, etc.) sont définis dans un fichier de configuration afin que l’outil puisse être adapté aux exigences de backport de n’importe quel projet sans modification du code.
* **Distribuable** — empaqueté sous la forme d’un wheel Python autonome installable depuis PyPI, sans dépendre de l’infrastructure d’intégration continue de ClickHouse.
* **Programmable** — expose un modèle objet clair pour les pull requests, les labels et les branches de release, afin que les utilisateurs puissent écrire des scripts de workflows personnalisés par-dessus le moteur principal.

<div id="testing">
  ### Tests
</div>

Une partie prévue de l’outil autonome est une suite de tests dédiée, accompagnée d’une infrastructure de test légère. Cette infrastructure pourra créer des dépôts GitHub temporaires (ou leurs équivalents locaux) préremplis avec :

* un ensemble configurable de branches représentant des lignes de release,
* des pull requests portant diverses combinaisons de labels de backport,
* des PR de release avec le label `release` pointant vers les branches de release.

Cela permet aux tests de couvrir l’ensemble de la boucle d’automatisation — détection des labels, création de branches de cherry-pick, gestion des conflits, création de pull requests de backport, logique d’assignation, exclusion en phase de rolling-out et politique de temporisation — sur un dépôt réel mais éphémère, sans toucher à l’état de production. La même infrastructure peut être réutilisée pour tester les régressions liées aux changements de policy avant leur déploiement.

<div id="active-release-branches">
  ## Branches de release actives
</div>

Une branche de release active est toute branche dont la release PR correspondante (portant le label `release`) est encore ouverte sur GitHub. L’automatisation des backports les détecte dynamiquement à chaque exécution, si bien qu’aucun changement de configuration n’est nécessaire lorsqu’une nouvelle release est publiée ou qu’une ancienne arrive en fin de vie.

Une branche de release peut être dans un état **rolling-out** (sa release PR porte le label `rolling-out`) pendant la période de déploiement d’une nouvelle release. Les backports généraux sont mis en pause pour les branches en rolling-out afin de ne pas compliquer le rollout. Les labels spécifiques à une version (par ex. `v25.3-must-backport`) l’emportent sur cette règle et forcent le backport même pendant un rollout.

Un label spécifique à une version définit la release la *plus ancienne* que la PR doit atteindre : elle est backportée vers cette release **ainsi que vers chaque branche de release active plus récente**, et pas uniquement vers celle qui est explicitement nommée. Par exemple, `v25.3-must-backport` sur une PR mergée dans la branche de développement entraîne un backport vers `25.3` ainsi que vers chaque release active ultérieure (`25.4`, `25.5`, …). Si plusieurs labels spécifiques à une version sont présents, c’est la version la plus basse qui l’emporte, puisqu’elle couvre déjà les versions plus récentes.

La release indiquée n’a pas besoin d’être elle-même active. Un label pour une release en fin de vie (sans release PR ouverte) propage quand même le correctif vers chaque release active ultérieure, de sorte qu’une mise à niveau depuis cette release ne perde jamais silencieusement le correctif. Par exemple, `v25.12-must-backport` sur une PR continue à être backporté vers `26.1`, `26.2`, … même après que `25.12` elle-même est arrivée en fin de vie.

<div id="implementation">
  ## Mise en œuvre
</div>

<div id="overview">
  ### Vue d’ensemble
</div>

L’automatisation du backport s’exécute toutes les heures via le workflow GitHub Actions `CherryPick` (`.github/workflows/cherry_pick.yml`), implémenté dans `tests/ci/cherry_pick.py`. Elle s’appuie sur l’API GitHub et sur des opérations git locales sur un runner `style-checker-aarch64` auto-hébergé.

Le processus se déroule en deux étapes pour chaque paire (PR d’origine, branche de release) :

1. Une **PR de cherry-pick** est créée afin d’isoler la résolution des conflits de la véritable cible de fusion. S’il n’y a pas de conflits, elle est fusionnée automatiquement.
2. Une **PR de backport** est créée sur la branche de release réelle, avec les modifications cherry-pickées regroupées en un seul commit.

<div id="labels">
  ### Labels
</div>

Les labels de la PR d’origine déterminent si un backport a lieu, et vers quelles branches.

| Label                                                  | Effet                                                                                                                                                                                                                                                                                                                                                                          |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `pr-must-backport`                                     | Backport vers toutes les branches de release actives (en ignorant celles marquées `rolling-out`)                                                                                                                                                                                                                                                                              |
| `pr-must-backport-force`                               | Backport vers toutes les branches de release actives, sans tenir compte des restrictions `rolling-out`                                                                                                                                                                                                                                                                        |
| `pr-critical-bugfix`                                   | Déclenche automatiquement `pr-must-backport` (via `AUTO_BACKPORT` dans `pr_labels_and_category.py`)                                                                                                                                                                                                                                                                            |
| `v{VER}-must-backport` (par ex. `v25.3-must-backport`) | Backport vers cette branche de release **et vers toutes les branches de release actives plus récentes** — la version indique la release *la plus ancienne* que la PR doit atteindre, même si la release nommée est elle-même en fin de vie. S’il y a plusieurs labels de ce type, la version la plus basse l’emporte. Remplace l’exclusion `rolling-out` pour ces branches |
| `pr-backports-created`                                 | Défini par le bot lorsque toutes les PR de backport requises ont été créées ; supprimé si une PR de cherry-pick est rouverte                                                                                                                                                                                                                                                  |
| `pr-cherrypick`                                        | Appliqué aux PR de cherry-pick créées par le bot                                                                                                                                                                                                                                                                                                                               |
| `pr-backport`                                          | Appliqué aux PR de backport créées par le bot                                                                                                                                                                                                                                                                                                                                  |
| `do not test`                                          | Appliqué aux PR de cherry-pick afin que l’intégration continue ne s’exécute pas sur celles-ci                                                                                                                                                                                                                                                                                  |
| `rolling-out`                                          | Défini sur une **release PR** pour indiquer que sa branche est en cours de déploiement ; les backports généraux l’ignorent                                                                                                                                                                                                                                                    |

<div id="branch-and-pr-naming">
  ### Nommage des branches et des PR
</div>

Pour chaque numéro de PR d’origine `N` et chaque branche de release `release/X.Y` :

* Branche de cherry-pick : `cherrypick/release/X.Y/N`
* Branche de backport : `backport/release/X.Y/N`
* Titre de la PR de cherry-pick : `Cherry pick #N to release/X.Y: <original title>`
* Titre de la PR de backport : `Backport #N to release/X.Y: <original title>`

<div id="step-by-step-process">
  ### Procédure étape par étape
</div>

<div id="discover-active-releases">
  #### 1. Découvrir les releases actives
</div>

`BackportPRs.receive_release_prs` interroge GitHub pour récupérer toutes les PR ouvertes portant le label `release`. Les refs `head` de ces PR correspondent aux noms des branches de release (par exemple `release/25.3`). À partir de là, il détermine l’ensemble des labels spécifiques à une version à rechercher : chaque label `v{VER}-must-backport` présent dans le dépôt dont la version n’est pas plus récente que la release active la plus récente. Les labels plus anciens sont inclus même lorsque leur release n’est plus active (un label plus récent que toutes les releases actives est ignoré, car il ne pourrait s’appliquer à aucune branche active), de sorte qu’une PR labellisée pour une release en fin de vie est quand même trouvée tant qu’une release plus récente est active.

<div id="find-prs-to-backport">
  #### 2. Trouver les PR à backporter
</div>

`BackportPRs.receive_prs_for_backport` utilise l&#39;API de recherche de GitHub pour trouver les PR fusionnées qui :

* portent au moins un label de backport (`pr-must-backport`, `pr-must-backport-force`, `pr-critical-bugfix` ou un label spécifique à une version), et
* n&#39;ont **pas** déjà le label `pr-backports-created`, et
* ont été fusionnées après la date du commit le plus ancien trouvé sur l&#39;une des branches de release, et
* ont été mises à jour au cours des 90 derniers jours (pour que la requête de recherche reste efficace).

<div id="rolling-out-branch-handling">
  #### 3. Gestion des branches rolling-out
</div>

Lorsqu’une PR de release porte le label `rolling-out`, les labels de backport généraux (`pr-must-backport`, `pr-critical-bugfix`) ignorent cette branche. Le bot ferme toutes les PR de cherry-pick ou de backport précédemment créées pour cette branche, avec un commentaire explicatif. Un label spécifique à la version (par ex. `v25.3-must-backport`) prévaut toujours — pour la release indiquée et pour chaque branche de release active plus récente qu’il couvre. `pr-must-backport-force` contourne la vérification `rolling-out` pour toutes les branches.

<div id="cherry-pick-stage">
  #### 4. Étape de cherry-pick (`ReleaseBranch.create_cherrypick`)
</div>

Pour chaque paire (PR d&#39;origine, branche de release) pour laquelle aucun PR de cherry-pick n&#39;existe encore :

1. Basculez sur la branche de release et créez une **branche de backport** (`backport/release/X.Y/N`) à partir de celle-ci.
2. Exécutez `git merge -s ours` sur le premier parent du commit de merge afin de créer une base de merge synthétique, sans modification du contenu.
3. Créez de force une **branche de cherry-pick** (`cherrypick/release/X.Y/N`) pointant directement vers le commit de merge du PR d&#39;origine.
4. Tentez un `git merge --no-commit --no-ff` de la branche de cherry-pick dans la branche de backport :
   * Si elle est déjà à jour, la modification est déjà présente dans la branche de release — marquez l&#39;étape comme terminée et passez à la suite.
   * Sinon (avec ou sans conflits), réinitialisez et poussez les deux branches.
5. Créez le PR de cherry-pick ciblant `backport/release/X.Y/N` depuis `cherrypick/release/X.Y/N`, avec les labels `pr-cherrypick` et `do not test`.
6. Propagez `pr-bugfix` ou `pr-critical-bugfix` depuis le PR d&#39;origine, le cas échéant.
7. Les personnes assignées ne sont **pas** définies à ce stade ; elles ne sont ajoutées que lorsque des conflits sont détectés.

<div id="auto-merge-conflict-free-cherry-pick-prs">
  #### 5. Fusion automatique des PR de cherry-pick sans conflits
</div>

Si la PR de cherry-pick est fusionnable (sans conflits), le bot la fusionne automatiquement via l’API GitHub et passe immédiatement à l’étape de backport.

<div id="backport-stage">
  #### 6. Étape de backport (`ReleaseBranch.create_backport`)
</div>

Une fois la PR de cherry-pick fusionnée :

1. Basculez sur la branche de backport, puis récupérez les dernières modifications.
2. Trouvez le `merge-base` entre la branche de release et la branche de backport.
3. Exécutez `git reset --soft` sur le `merge-base` afin de regrouper tous les commits de cherry-pick en un seul.
4. Effectuez un commit en utilisant le titre de la PR de backport comme message.
5. Force-pushez la branche de backport et ouvrez une PR de backport ciblant la véritable branche de release.
6. Ajoutez à la PR le label `pr-backport` (ainsi que `pr-bugfix` / `pr-critical-bugfix`, le cas échéant).
7. Assignez la PR à l’auteur de la PR d’origine, à la personne qui l’a fusionnée et aux personnes déjà assignées (hors comptes robot).

<div id="completion">
  #### 7. Finalisation
</div>

Lorsque toutes les branches de release associées à une PR d&#39;origine donnée ont été backportées, le bot ajoute `pr-backports-created` à la PR d&#39;origine.

<div id="pre-check">
  #### 8. Vérification préalable
</div>

Avant de commencer à travailler sur une PR, `ReleaseBranch.pre_check` exécute `git merge-base --is-ancestor` pour vérifier que le commit de fusion n’est pas déjà atteignable depuis la branche de release. Si c’est le cas, la PR est considérée comme déjà backportée et est ignorée.

<div id="stale-cherry-pick-pr-handling">
  ### Gestion des PR de cherry-pick obsolètes
</div>

La classe `CherryPickPRs` s’exécute au début de chaque exécution horaire et gère deux cas de figure :

* **PR de cherry-pick orphelines** : si la branche de release d’une PR de cherry-pick n’a plus de PR de release ouverte (c.-à-d. que la release est fermée), la PR de cherry-pick est fermée automatiquement.
* **PR de cherry-pick rouvertes** : si une PR d’origine a déjà le label `pr-backports-created`, mais qu’une PR de cherry-pick associée est toujours ouverte, le label `pr-backports-created` est retiré de la PR d’origine afin qu’elle puisse être retraitée.

Pour les PR de cherry-pick en attente d’une résolution manuelle des conflits :

* Après **3 jours** sans mise à jour, le bot publie un commentaire de relance en mentionnant les personnes assignées.
* Après **7 jours** sans mise à jour, le bot publie un commentaire de fermeture et ferme la PR.

<div id="conflict-resolution">
  ### Résolution des conflits
</div>

Lorsqu&#39;un `cherry-pick` entraîne des conflits, la PR de cherry-pick reste ouverte afin qu&#39;une personne puisse les résoudre. Le bot l&#39;assigne à l&#39;auteur de la PR d&#39;origine, à la personne qui a fusionné la PR, ainsi qu&#39;aux personnes assignées. Une fois les conflits résolus et la PR de cherry-pick fusionnée, le bot crée la PR de backport lors de l&#39;exécution horaire suivante.

Pour abandonner complètement un backport, fermez la PR de cherry-pick. Le bot la considérera comme volontairement ignorée.

Pour recréer une PR de cherry-pick défectueuse à partir de zéro :

1. Supprimez le label `pr-cherrypick` de la PR de cherry-pick.
2. Supprimez la branche `cherrypick/...`.
3. Supprimez `pr-backports-created` de la PR d&#39;origine s&#39;il est présent.

<div id="ci-for-backport-prs">
  ### Intégration continue pour les PR de backport
</div>

Les PR de backport ciblent des branches de release et utilisent donc un workflow d’intégration continue dédié (`BackportPR`, défini dans `ci/workflows/backport_branches.py`) plutôt que le workflow de pull request standard. Ce workflow exécute un sous-ensemble représentatif de l’intégration continue : builds ASan/UBSan et TSan, builds de release, builds macOS, tests fonctionnels sous ASan, tests de stress sous TSan et tests d’intégration. Il vérifie que la branche de backport contient entre 1 et 50 commits et au moins un fichier modifié (vérification assurée par `check_backport_branch.py`).

<div id="authentication">
  ### Authentification
</div>

Le workflow utilise une clé SSH (`ROBOT_CLICKHOUSE_SSH_KEY`) pour les opérations de `git push`. Les appels à l’API GitHub s’authentifient via `get_best_robot_token`, qui sélectionne le jeton disposant du quota restant le plus élevé dans un ensemble stocké dans SSM (`/github-tokens`). `ROBOT_CLICKHOUSE_COMMIT_TOKEN` est utilisé par l’étape de checkout du workflow Actions, et non pour les appels à l’API. Les comptes robot (`robot-clickhouse`, `clickhouse-gh`) sont exclus lors de l’attribution d’un responsable.

<div id="github-api-cache">
  ### Cache de l’API GitHub
</div>

`GitHubCache` (issu de `cache_utils.py`) conserve le cache d’objets PyGithub dans S3, ce qui réduit les appels à l’API d’une exécution horaire à l’autre. Le cache est téléchargé au début, puis téléversé à la fin de chaque exécution.

<div id="error-handling">
  ### Gestion des erreurs
</div>

Les erreurs survenant lors du traitement de chaque PR sont interceptées et consignées, mais n’interrompent pas l’exécution. Une fois tous les PR traités, si des erreurs se sont produites, une `BackportException` est levée. En intégration continue, cela déclenche une notification via `CIBuddy` dans le chat de l’équipe.