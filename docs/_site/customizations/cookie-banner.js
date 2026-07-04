(function () {
  'use strict';

  var COOKIE_NAME = 'ch_cookie_consent';
  var CONSENT_ACCEPTED = 'accepted';
  var CONSENT_REJECTED = 'rejected';

  function getConsent() {
    var match = document.cookie.match(new RegExp('(^| )' + COOKIE_NAME + '=([^;]+)'));
    return match ? match[2] : null;
  }

  function setConsent(value) {
    var d = new Date();
    d.setTime(d.getTime() + 365 * 24 * 60 * 60 * 1000);
    document.cookie = COOKIE_NAME + '=' + value + ';expires=' + d.toUTCString() + ';path=/;SameSite=Lax';
  }

  function enableAnalytics() {
    // Fire a custom event that GTM / GA can listen for
    window.dataLayer = window.dataLayer || [];
    window.dataLayer.push({
      event: 'cookie_consent_granted',
      analytics_storage: 'granted'
    });
  }

  function disableAnalytics() {
    window.dataLayer = window.dataLayer || [];
    window.dataLayer.push({
      event: 'cookie_consent_denied',
      analytics_storage: 'denied'
    });
  }

  // Set default consent state (denied until user opts in)
  window.dataLayer = window.dataLayer || [];
  function gtag() { window.dataLayer.push(arguments); }
  gtag('consent', 'default', {
    analytics_storage: 'denied',
    ad_storage: 'denied',
    wait_for_update: 500
  });

  // If user already consented, enable analytics immediately
  var existing = getConsent();
  if (existing === CONSENT_ACCEPTED) {
    gtag('consent', 'update', { analytics_storage: 'granted' });
    enableAnalytics();
  } else if (existing === CONSENT_REJECTED) {
    disableAnalytics();
  }

  // Cookie SVG icon (matches screenshot)
  var cookieSvg = '<svg width="48" height="48" viewBox="0 0 48 48" fill="none" xmlns="http://www.w3.org/2000/svg">'
    + '<path d="M24 4C12.954 4 4 12.954 4 24s8.954 20 20 20 20-8.954 20-20c0-1.1-.09-2.18-.26-3.23a1 1 0 0 0-1.18-.82 5 5 0 0 1-5.82-6.2 1 1 0 0 0-.56-1.1A8 8 0 0 1 32 5.5a1 1 0 0 0-.78-1.2A20.1 20.1 0 0 0 24 4Z" stroke="#fdff75" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>'
    + '<circle cx="16" cy="20" r="2" fill="#fdff75"/>'
    + '<circle cx="24" cy="28" r="2" fill="#fdff75"/>'
    + '<circle cx="20" cy="34" r="1.5" fill="#fdff75"/>'
    + '<circle cx="30" cy="20" r="1.5" fill="#fdff75"/>'
    + '<circle cx="14" cy="28" r="1.5" fill="#fdff75"/>'
    + '</svg>';

  function showBanner() {
    // Don't show if consent already given
    if (getConsent()) return;

    var overlay = document.createElement('div');
    overlay.id = 'ch-cookie-overlay';
    overlay.style.cssText = 'position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.5);z-index:99999;display:flex;align-items:center;justify-content:center;padding:16px;';

    var banner = document.createElement('div');
    banner.id = 'ch-cookie-banner';
    banner.style.cssText = 'background:#2a2a2a;border-radius:16px;padding:32px 28px 28px;max-width:400px;width:100%;text-align:center;box-shadow:0 8px 32px rgba(0,0,0,0.4);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;';

    banner.innerHTML = '<div style="margin-bottom:16px;display:flex;justify-content:center;">' + cookieSvg + '</div>'
      + '<h3 style="color:#fff;font-size:20px;font-weight:700;margin:0 0 12px;line-height:1.3;">Could we interest you in a cookie?</h3>'
      + '<p style="color:#b0b0b0;font-size:14px;line-height:1.6;margin:0 0 24px;">'
      + 'ClickHouse uses cookies to make your experience extra sweet! Some keep things running smoothly (essential cookies), while others help us improve our site (analytics cookies). '
      + '<a href="https://clickhouse.com/legal/privacy-policy" target="_blank" rel="noopener" style="color:#fdff75;text-decoration:none;">Learn more</a>'
      + '</p>'
      + '<div style="display:flex;gap:12px;justify-content:center;">'
      + '<button id="ch-cookie-accept" style="background:#fdff75;color:#1c1c1c;border:none;border-radius:8px;padding:10px 24px;font-size:14px;font-weight:600;cursor:pointer;transition:opacity 0.2s;">Accept cookies</button>'
      + '<button id="ch-cookie-reject" style="background:transparent;color:#fff;border:1px solid #555;border-radius:8px;padding:10px 24px;font-size:14px;font-weight:600;cursor:pointer;transition:opacity 0.2s;">Reject cookies</button>'
      + '</div>';

    overlay.appendChild(banner);
    document.body.appendChild(overlay);

    document.getElementById('ch-cookie-accept').addEventListener('click', function () {
      setConsent(CONSENT_ACCEPTED);
      gtag('consent', 'update', { analytics_storage: 'granted' });
      enableAnalytics();
      overlay.remove();
    });

    document.getElementById('ch-cookie-reject').addEventListener('click', function () {
      setConsent(CONSENT_REJECTED);
      disableAnalytics();
      overlay.remove();
    });
  }

  // Show banner once DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', showBanner);
  } else {
    showBanner();
  }
})();