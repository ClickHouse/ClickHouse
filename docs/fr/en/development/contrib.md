---
description: 'Page décrivant l’utilisation par ClickHouse de composants tiers et la
  manière d’ajouter et de maintenir des bibliothèques tierces.'
sidebar_label: 'Bibliothèques tierces'
sidebar_position: 60
slug: /development/contrib
title: 'Bibliothèques tierces'
doc_type: 'reference'
---

ClickHouse utilise des bibliothèques tierces à diverses fins, par exemple pour se connecter à d’autres bases de données, pour décoder/encoder des données lors de leur chargement depuis le disque ou de leur enregistrement sur le disque, ou pour implémenter certaines fonctions SQL spécialisées.
Afin de ne pas dépendre des bibliothèques disponibles sur le système cible, chaque bibliothèque tierce est importée dans l’arborescence des sources de ClickHouse en tant que sous-module Git, puis compilée et liée avec ClickHouse.
La liste des bibliothèques tierces et de leurs licences peut être obtenue à l’aide de la requête suivante :

```sql
SELECT library_name, license_type, license_path FROM system.licenses ORDER BY library_name COLLATE 'en';
```

Notez que les bibliothèques répertoriées sont celles qui se trouvent dans le répertoire `contrib/` du dépôt ClickHouse.
Selon les options de compilation, il se peut que certaines de ces bibliothèques n’aient pas été compilées et que, par conséquent, leurs fonctionnalités ne soient pas disponibles à l’exécution.

[Exemple](https://sql.clickhouse.com?query_id=478GCPU7LRTSZJBNY3EJT3)

<div id="adding-and-maintaining-third-party-libraries">
  ## Ajout et maintenance des bibliothèques tierces
</div>

Chaque bibliothèque tierce doit se trouver dans un répertoire dédié sous le répertoire `contrib/` du dépôt ClickHouse.
Évitez d&#39;y copier directement du code externe.
Créez plutôt un sous-module Git pour récupérer le code tiers depuis un dépôt externe en amont.

Tous les sous-modules utilisés par ClickHouse sont listés dans le fichier `.gitmodule`.

* Si la bibliothèque peut être utilisée telle quelle (cas par défaut), vous pouvez référencer directement le dépôt en amont.
* Si la bibliothèque nécessite des correctifs, créez un fork du dépôt en amont dans l&#39;[organisation ClickHouse sur GitHub](https://github.com/ClickHouse).

Dans ce dernier cas, nous cherchons à isoler autant que possible les correctifs personnalisés des commits en amont.
Pour cela, créez une branche avec le préfixe `ClickHouse/` à partir de la branche ou du tag que vous souhaitez intégrer, par exemple `ClickHouse/2024_2` (pour la branche `2024_2`) ou `ClickHouse/release/vX.Y.Z` (pour le tag `release/vX.Y.Z`).
Évitez de suivre les branches de développement en amont `master`/ `main` / `dev` (c.-à-d. les branches préfixées `ClickHouse/master` / `ClickHouse/main` / `ClickHouse/dev` dans le dépôt forké).
Ces branches évoluent en permanence, ce qui complique un versionnement correct.
Les « branches préfixées » garantissent que les pulls du dépôt en amont vers le fork laisseront les branches personnalisées `ClickHouse/` inchangées.
Les sous-modules dans `contrib/` ne doivent suivre que les branches `ClickHouse/` des dépôts tiers forkés.

Les correctifs ne sont appliqués que sur les branches `ClickHouse/` des bibliothèques externes.

Il y a deux façons de procéder :

* vous souhaitez créer un nouveau correctif sur une branche préfixée par `ClickHouse/` dans le dépôt forké, par exemple un correctif de sanitizer. Dans ce cas, poussez le correctif sous la forme d&#39;une branche avec le préfixe `ClickHouse/`, par exemple `ClickHouse/fix-sanitizer-disaster`. Créez ensuite une PR de cette nouvelle branche vers la branche de suivi personnalisée, par exemple `ClickHouse/2024_2 <-- ClickHouse/fix-sanitizer-disaster`, puis fusionnez la PR.
* vous mettez à jour le sous-module et devez réappliquer d&#39;anciens correctifs. Dans ce cas, recréer d&#39;anciennes PR serait excessif. À la place, faites simplement un cherry-pick des anciens commits dans la nouvelle branche `ClickHouse/` (correspondant à la nouvelle version). N&#39;hésitez pas à squash les commits des PR qui en comportaient plusieurs. Dans le meilleur des cas, nous aurons déjà contribué les correctifs personnalisés au dépôt en amont et pourrons les omettre dans la nouvelle version.

Une fois le sous-module mis à jour, mettez à jour le sous-module dans ClickHouse pour qu&#39;il pointe vers le nouveau hash dans le fork.

Créez des correctifs pour les bibliothèques tierces en gardant le dépôt officiel à l&#39;esprit et envisagez de les contribuer au dépôt en amont.
Ainsi, d&#39;autres en bénéficieront également et cela ne représentera pas une charge de maintenance pour l&#39;équipe ClickHouse.