export const TrackedLink = ({ href, eventName, children, ...rest }) => {
  const handleClick = () => {
    try {
      if (typeof window !== "undefined" && window.galaxy && eventName) {
        window.galaxy.track(eventName, { interaction: "click" });
      }
    } catch (e) {
      // Falhas no rastreamento não devem interromper a navegação.
    }
  };
  return (
    <a href={href} onClick={handleClick} {...rest}>
      {children}
    </a>
  );
};