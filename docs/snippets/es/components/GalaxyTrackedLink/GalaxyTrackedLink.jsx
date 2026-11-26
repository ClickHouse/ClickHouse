export const TrackedLink = ({ href, eventName, children, ...rest }) => {
  const handleClick = () => {
    try {
      if (typeof window !== "undefined" && window.galaxy && eventName) {
        window.galaxy.track(eventName, { interaction: "click" });
      }
    } catch (e) {
      // Los errores de seguimiento no deben interrumpir la navegación.
    }
  };
  return (
    <a href={href} onClick={handleClick} {...rest}>
      {children}
    </a>
  );
};