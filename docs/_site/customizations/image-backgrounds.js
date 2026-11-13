(function () {
  'use strict';

  // Mintlify creates a separate image under `document.body` for the click-to-
  // zoom overlay. The copy does not retain the page image's wrapper classes,
  // so a configured background for transparent assets would otherwise be lost.
  function imagePath(image) {
    var source = image.currentSrc || image.src;
    return source ? new URL(source, window.location.href).pathname : '';
  }

  function isTransparent(color) {
    return color === 'transparent' || color === 'rgba(0, 0, 0, 0)';
  }

  function pageImageFor(zoomImage) {
    var path = imagePath(zoomImage);
    if (!path) return null;

    var images = document.querySelectorAll('img');
    for (var index = 0; index < images.length; index += 1) {
      var image = images[index];
      if (image === zoomImage || image.closest('[data-ramka-media]')) continue;
      if (imagePath(image) !== path) continue;
      if (image.alt !== zoomImage.alt) continue;

      var backgroundColor = window.getComputedStyle(image).backgroundColor;
      if (!isTransparent(backgroundColor)) return backgroundColor;
    }

    return null;
  }

  function preserveBackground(zoomImage) {
    if (!zoomImage.matches('[data-ramka-media] > img')) return;

    var backgroundColor = pageImageFor(zoomImage);
    if (backgroundColor) zoomImage.style.backgroundColor = backgroundColor;
  }

  function preserveBackgrounds(root) {
    if (root instanceof Element && root.matches('[data-ramka-media] > img')) {
      preserveBackground(root);
    }

    if (!(root instanceof Element || root instanceof Document || root instanceof DocumentFragment)) return;
    var images = root.querySelectorAll('[data-ramka-media] > img');
    for (var index = 0; index < images.length; index += 1) {
      preserveBackground(images[index]);
    }
  }

  preserveBackgrounds(document);

  var observer = new MutationObserver(function (records) {
    for (var recordIndex = 0; recordIndex < records.length; recordIndex += 1) {
      var record = records[recordIndex];
      if (record.type === 'attributes') {
        preserveBackground(record.target);
        continue;
      }

      for (var nodeIndex = 0; nodeIndex < record.addedNodes.length; nodeIndex += 1) {
        preserveBackgrounds(record.addedNodes[nodeIndex]);
      }
    }
  });

  observer.observe(document.body, {
    childList: true,
    subtree: true,
    attributes: true,
    attributeFilter: ['src'],
  });
})();
