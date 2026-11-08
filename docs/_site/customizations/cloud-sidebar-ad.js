(function () {
  'use strict';

  var AD_SLOT_ID = 'ch-cloud-sidebar-ad-slot';
  var DISMISSED_KEY = 'ch-cloud-sidebar-ad-dismissed';
  var MCP_LINK_PATH = '/set-up-clickhouse-documentation-mcp-server';
  var SIGNUP_HREF = 'https://clickhouse.cloud/signUp?loc=docs-card-banner';
  var dismissedForPage = false;
  var AD_COPY = {
    en: {
      ariaLabel: 'ClickHouse Cloud advert',
      dismissLabel: 'Dismiss ClickHouse Cloud advert permanently',
      title: 'Try ClickHouse Cloud for FREE',
      description: 'Separation of storage and compute, automatic scaling, built-in SQL console, and lots more. $300 in free credits when signing up.',
      linkLabel: 'Try it for Free',
    },
    ar: {
      ariaLabel: 'إعلان ClickHouse Cloud',
      dismissLabel: 'إخفاء إعلان ClickHouse Cloud نهائيًا',
      title: 'جرّب ClickHouse Cloud مجانًا',
      description: 'فصل التخزين عن الحوسبة، والتوسّع التلقائي، ووحدة تحكم SQL مضمّنة، وغير ذلك الكثير. احصل على رصيد مجاني بقيمة 300 دولار عند التسجيل.',
      linkLabel: 'جرّبه مجانًا',
    },
    es: {
      ariaLabel: 'Anuncio de ClickHouse Cloud',
      dismissLabel: 'Descartar permanentemente el anuncio de ClickHouse Cloud',
      title: 'Prueba ClickHouse Cloud GRATIS',
      description: 'Separación de almacenamiento y cómputo, escalado automático, consola SQL integrada y mucho más. Obtén 300 USD en créditos gratis al registrarte.',
      linkLabel: 'Pruébalo gratis',
    },
    fr: {
      ariaLabel: 'Annonce ClickHouse Cloud',
      dismissLabel: 'Masquer définitivement l’annonce ClickHouse Cloud',
      title: 'Essayez ClickHouse Cloud GRATUITEMENT',
      description: 'Séparation du stockage et du calcul, mise à l’échelle automatique, console SQL intégrée et bien plus encore. Recevez 300 $ de crédits gratuits lors de votre inscription.',
      linkLabel: 'Essayer gratuitement',
    },
    ja: {
      ariaLabel: 'ClickHouse Cloud の広告',
      dismissLabel: 'ClickHouse Cloud の広告を今後表示しない',
      title: 'ClickHouse Cloud を無料でお試しください',
      description: 'ストレージとコンピューティングの分離、自動スケーリング、組み込み SQL コンソールなどを利用できます。登録時に 300 ドル分の無料クレジットを進呈します。',
      linkLabel: '無料で試す',
    },
    ko: {
      ariaLabel: 'ClickHouse Cloud 광고',
      dismissLabel: 'ClickHouse Cloud 광고를 영구적으로 닫기',
      title: 'ClickHouse Cloud를 무료로 사용해 보세요',
      description: '스토리지와 컴퓨팅 분리, 자동 확장, 기본 제공 SQL 콘솔 등 다양한 기능을 제공합니다. 가입하면 300달러의 무료 크레딧을 받을 수 있습니다.',
      linkLabel: '무료로 사용해 보기',
    },
    'pt-BR': {
      ariaLabel: 'Anúncio do ClickHouse Cloud',
      dismissLabel: 'Dispensar permanentemente o anúncio do ClickHouse Cloud',
      title: 'Experimente o ClickHouse Cloud GRÁTIS',
      description: 'Separação de armazenamento e computação, escalonamento automático, console SQL integrado e muito mais. Receba US$ 300 em créditos grátis ao se cadastrar.',
      linkLabel: 'Experimente grátis',
    },
  };

  function normalizedPath() {
    return window.location.pathname.replace(/^\/docs(?=\/|$)/, '');
  }

  function getAdCopy() {
    var localeMatch = normalizedPath().match(/^\/(ar|es|fr|ja|ko|pt-BR)(?:\/|$)/);
    return localeMatch ? AD_COPY[localeMatch[1]] : AD_COPY.en;
  }

  function isGetStartedPage() {
    var path = normalizedPath();
    if (/^\/(?:ru|zh)(?:\/|$)/.test(path)) return false;
    return /^\/(?:(?:ar|es|fr|ja|ko|pt-BR|ru|zh)\/)?get-started(?:\/|$)/.test(path);
  }

  function storageGet(key) {
    try {
      return window.localStorage.getItem(key);
    } catch (e) {
      return null;
    }
  }

  function storageSet(key, value) {
    try {
      window.localStorage.setItem(key, value);
    } catch (e) { /* Dismiss for the current page even if storage is unavailable. */ }
  }

  function track(eventName, href) {
    if (!window.galaxy || typeof window.galaxy.track !== 'function') return;
    window.galaxy.track(eventName, {
      interaction: 'click',
      href: href,
    });
  }

  function findMcpControl() {
    var controls = document.querySelectorAll('#table-of-contents a[href], #table-of-contents button');
    for (var i = 0; i < controls.length; i++) {
      var href = controls[i].getAttribute('href') || '';
      var text = controls[i].textContent || '';
      if (href.indexOf(MCP_LINK_PATH) !== -1 || text.indexOf('ClickHouse documentation MCP server') !== -1) {
        return controls[i];
      }
    }
    return null;
  }

  function updateText(element, value) {
    if (element && element.textContent !== value) {
      element.textContent = value;
    }
  }

  function updateAdCopy(slot) {
    var copy = getAdCopy();
    var card = slot.querySelector('.ch-cloud-sidebar-ad');
    if (card && card.getAttribute('aria-label') !== copy.ariaLabel) {
      card.setAttribute('aria-label', copy.ariaLabel);
    }

    var dismissButton = slot.querySelector('.ch-cloud-sidebar-ad-dismiss');
    if (dismissButton && dismissButton.getAttribute('aria-label') !== copy.dismissLabel) {
      dismissButton.setAttribute('aria-label', copy.dismissLabel);
    }
    updateText(slot.querySelector('.ch-cloud-sidebar-ad-title'), copy.title);
    updateText(slot.querySelector('.ch-cloud-sidebar-ad-description'), copy.description);
    updateText(slot.querySelector('.ch-cloud-sidebar-ad-link'), copy.linkLabel);
  }

  function createAdSlot(tagName) {
    var slot = document.createElement(tagName);
    slot.id = AD_SLOT_ID;
    slot.className = 'ch-cloud-sidebar-ad-slot';

    var card = document.createElement('aside');
    card.className = 'ch-cloud-sidebar-ad';

    var dismissButton = document.createElement('button');
    dismissButton.className = 'ch-cloud-sidebar-ad-dismiss';
    dismissButton.type = 'button';
    dismissButton.textContent = '\u00d7';
    dismissButton.onclick = function () {
      dismissedForPage = true;
      storageSet(DISMISSED_KEY, 'true');
      track('docs.sidebarCloudAdvert.advertDismissed', SIGNUP_HREF);
      slot.remove();
    };

    var title = document.createElement('p');
    title.className = 'ch-cloud-sidebar-ad-title';

    var description = document.createElement('p');
    description.className = 'ch-cloud-sidebar-ad-description';

    var link = document.createElement('a');
    link.className = 'ch-cloud-sidebar-ad-link';
    link.href = SIGNUP_HREF;
    link.target = '_blank';
    link.rel = 'noopener noreferrer';
    link.onclick = function () {
      track('docs.sidebarCloudAdvert.clickedThrough', SIGNUP_HREF);
    };

    card.appendChild(dismissButton);
    card.appendChild(title);
    card.appendChild(description);
    card.appendChild(link);
    slot.appendChild(card);
    updateAdCopy(slot);
    return slot;
  }

  function injectAd() {
    var existing = document.getElementById(AD_SLOT_ID);
    if (!isGetStartedPage()) {
      if (existing) existing.remove();
      return true;
    }
    if (dismissedForPage || storageGet(DISMISSED_KEY) === 'true') {
      if (existing) existing.remove();
      return true;
    }
    if (existing) {
      updateAdCopy(existing);
      return true;
    }

    var mcpControl = findMcpControl();
    if (!mcpControl) return false;

    var placement = mcpControl.closest('li') || mcpControl;
    var slotTagName = placement.tagName === 'LI' ? 'li' : 'div';
    placement.insertAdjacentElement('afterend', createAdSlot(slotTagName));
    return true;
  }

  function init() {
    injectAd();

    var scheduled = false;
    new MutationObserver(function () {
      if (scheduled) return;
      scheduled = true;
      requestAnimationFrame(function () {
        scheduled = false;
        injectAd();
      });
    }).observe(document.documentElement, { childList: true, subtree: true });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
