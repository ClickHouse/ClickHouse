/**
 * Renders compatibility content only when the current URL targets one of the
 * supplied legacy anchors.
 */
export const LegacyDeepLinkSection = ({ anchorIds, accordionId, children }) => {
  const [targetId, setTargetId] = useState(null);

  useEffect(() => {
    const updateTarget = () => {
      const currentTarget = window.location.hash.slice(1);
      const isCompatibilityTarget = anchorIds.includes(currentTarget) || currentTarget === accordionId;
      setTargetId(isCompatibilityTarget ? currentTarget : null);
    };

    updateTarget();
    window.addEventListener('hashchange', updateTarget);

    return () => window.removeEventListener('hashchange', updateTarget);
  }, [accordionId, anchorIds]);

  useEffect(() => {
    if (!targetId) return;

    const animationFrame = window.requestAnimationFrame(() => {
      const target = document.getElementById(targetId);
      const details = target?.closest('details');

      if (!target || !details) return;

      details.open = true;
      target.scrollIntoView({ block: 'start' });
    });

    return () => window.cancelAnimationFrame(animationFrame);
  }, [targetId]);

  if (!targetId) return null;

  return children;
};
